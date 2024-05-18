#-*- coding: utf-8 -*-

#
#Copyright APC, CNRS, Université Paris Diderot, 2008
#
#http://www.apc.univ-paris7.fr
#https://forge.in2p3.fr/projects/apc-scheduler
#
#Authors:
#   Colley Jean-Marc <colley@in2p3.fr>
#
#This software is a computer program whose purpose to simply
#job submission on grid with gLite/WMS middleware :
# * creation binary tarball, included specified library
# * retrieve data in mail box directory on CE to UI user directory
# * resubmit "zombi" job on different WMS after time-out defined by used
# * management of mpi job with MPI-START method
# * 
#
#This software is governed by the CeCILL-B license under French law and
#abiding by the rules of distribution of free software.  You can  use, 
#modify and/ or redistribute the software under the terms of the CeCILL-B
#license as circulated by CEA, CNRS and INRIA at the following URL
#"http://www.cecill.info". 
#
#As a counterpart to the access to the source code and  rights to copy,
#modify and redistribute granted by the license, users are provided only
#with a limited warranty  and the software's author,  the holder of the
#economic rights,  and the successive licensors  have only  limited
#liability. 
#
#In this respect, the user's attention is drawn to the risks associated
#with loading,  using,  modifying and/or developing or reproducing the
#software by the user in light of its specific status of free software,
#that may mean  that it is complicated to manipulate,  and  that  also
#therefore means  that it is reserved for developers  and  experienced
#professionals having in-depth computer knowledge. Users are therefore
#encouraged to load and test the software's suitability as regards their
#requirements in conditions enabling the security of their systems and/or 
#data to be ensured and,  more generally, to use and operate it in the 
#same conditions as regards security. 
#
#The fact that you are presently reading this means that you have had
#knowledge of the CeCILL-B license and that you accept its terms.
#



import time
import string
import threading
from random import choice
import popen2
import os
import sys
import types 
import random as rd
import operator
import signal
import re

###############################################################
#
#  Global variable
#
###############################################################

G_verbose = 2
G_exit = False

###############################################################
#
#  Tool function
#
###############################################################

def sendMailFile(File):
    """Mail a file """
    if MyConf().info.mail == '':
        print 'Define address mail with setMail() function'
        return
    ret = os.path.isfile(File)
    if ret:
        os.system('mail -s "APCScheduler : %s" %s < %s'%(File, MyConf().info.mail, File))
    else:
        print 'File "'+File+'" doesn\'t exist'


def newJob(MyAppli, scheduler='LOCAL'):
    """Return Job object with follow default scheduler [LOCAL]"""
    if scheduler == 'GLITE':
        MySched = SchedulerGLITE()
    elif scheduler == 'LOCAL':
        MySched = SchedulerLOCAL()
    elif scheduler == 'BQS':
        MySched = SchedulerBQS()
    elif scheduler == 'GE':
        MySched = SchedulerSGE()
    else:
        print 'Error unknown scheduler !!'
        return None
    return JobClass(MyAppli, MySched)


def setMail(mail):
    """ Define mail address"""
    # To do: check if repository is OK   
    MyConf().info.mail = mail

    
def setFileConfVO(fileconf):
    """ Define mail address"""
    file_tmp = os.path.expanduser(fileconf)
    ret = os.path.isfile(file_tmp)
    if ret:
        ConfigGrid().setFileConf(file_tmp)
    else:
        sys.stderr.write("\nERROR: file '%s' doesn't exist !\n"%fileconf) 


def setRepository(NameRepository):
    """ Define repository, create it if necessary"""
    # To do: check if repository is OK
    global S_GridRepository
    if NameRepository[0] != '/':
        NameRepository = os.path.join(os.getcwd(),NameRepository)
    if NameRepository[-1] != '/':
        NameRepository +=  '/'

    if not os.path.isdir(NameRepository):
        # Create directory
        os.makedirs(NameRepository)
    MyConf().info.workDir = NameRepository


def setVerboseLevel(level):
    """Set verbose level [0-20]: 0 no message, 20 all messages"""
    global G_verbose
    if level >= 0 and level <= 20:
        G_verbose = level
    else:
        print "Level must be between 0 and 20."


def submitCmd(cmd, TpsMax=3600, verbose=True):
    myp = Process(cmd)
    if G_verbose > 4: print cmd
    myp.wait(TpsMax)         
    if verbose or not myp.isOk():
        sys.stderr.write(myp.stdOut())   
        sys.stderr.write(myp.stdErr())
    return myp.getExitValue()


def addLIBRARYPATH(path):
    if os.environ.has_key('LD_LIBRARY_PATH'):
        os.environ['LD_LIBRARY_PATH'] += ':'+path
    else:
        os.environ['LD_LIBRARY_PATH'] = path


def getVersion():
    return "APCscheduler june 2012, version: 0.9.3"


#
# Private
#

def _initProxy(nb_hours=72):
    # peut détruire des proxys ?! à voir 
    os.system('myproxy-destroy -d')
    os.system('glite-voms-proxy-destroy')

    if nb_hours <= 0:
        sys.stderr.write("\nERROR: enter a positive number\n")
        sys.exit(1)    
    return os.system('glite-voms-proxy-init -voms %s -hours %d'%(MyConf().gLite.vo, nb_hours))
    
    
def _cancelJob(signum, frame):
    print 'Signal handler called with signal', signum
    print frame
    global G_exit
    G_exit = True
    for job in JobClass.dictJobSubmit.values():
        try:
            print "try cancel job ", job.name()
            job._Scheduler.cancel(job._Appli)
        except:
            pass
    sys.exit(signum)
        

signal.signal(signal.SIGINT, _cancelJob)
#signal.signal(signal.SIGKILL, _cancelJob)

def _AddAlea(MyStr,NbChar):
    Alea = ''
    Alphabet = string.letters + string.digits
    for i in range(NbChar):
        Alea += choice(Alphabet)
    MyStr += '_'+Alea
    return(MyStr)


def _ListUnique(MyList):
    InternList = []
    for elt in MyList:
        if elt not in InternList:
            InternList.append(elt)
    return(InternList)


def _GetWordsAfter(buffer, keyword):
    idx = buffer.find(keyword)
    if idx >= 0:
        # conserve tout apres keyword
        next = buffer[idx+len(keyword):]        
        # decoupe par ligne
        next = next.split('\n')
        #print next 
        # suppression chaine vide
        next.remove('')
        #print next 
        # extrait premier mot de la premiere ligne
        next = next[0].split()
        #print next       
        return next[0]      
    return None


def _AbsolutePathExe(MyExe):
    val = MyExe.find('/')
    if val >= 0:
        if val == 0:
            if os.access(MyExe, os.X_OK):
                return MyExe
            else:
                return None
        if MyExe[0:2] == './':
            # exe local
            file = os.getcwd()+MyExe[1:]
            if os.access(file, os.X_OK):
                return file
            return None
        else:
        #elif MyExe[0:2] == '..':
            # exe relatif 
            file = os.getcwd()+'/'+MyExe
            if os.access(file, os.X_OK):
                return file
            return None            
    else:        
        if os.access(MyExe, os.X_OK):
            return MyExe
        else:
            pathlist = os.getenv('PATH').split(os.pathsep)
            for path in pathlist:        
                file = os.path.join(path, MyExe)
                #print file
                if os.access(file, os.X_OK):
                    if path == '.':                   
                        file = os.getcwd()+'/'+  MyExe
                    return file
            return None


def _ConvHHMMSSInSec(time):
    dec = time.split(':')
    ToSec = 1
    TotalInSec = 0
    for i in range(len(dec)-1,-1,-1):        
        TotalInSec += long(dec[i])*ToSec        
        ToSec *= 60 
    return TotalInSec


def _FindLib(exelib, listlib):
    myp = Process('ldd '+ exelib)
    myp.wait()
    output = myp.stdOut()
    setlines =output.split('\n')
    setlines.remove('')
    for line in setlines:
        if line.find('=>') != -1:
            lib = line.split()[2]
            if (lib=='not'):
                print "path lib not find !! Update LD_LIBRARY_PATH var env "
                return False
        else:
            lib = line.split()[0]       
        if (lib.find('/lib64') != 0) and (lib.find('/usr/lib64') != 0):
            PasLa = True
            # pas de doublon 
            for elt in listlib:
                if elt == lib :
                    PasLa = False
                    break
            if PasLa:
                listlib.append(lib)                
                ret = _FindLib(lib, listlib)
                if not ret:
                    return ret    
    return True


def _readFile(file):
    try:
        pf=open(file,'r')
        #print "OpenFile "+file
    except IOError:
        #print "file %s doesn'MPIt exist"%file
        return None
    buf = pf.read()
    pf.close()
    return buf

def _writeFile(file,buf):
    try:
        pf=open(file,'a')
        #print "OpenFile "+file
    except IOError:
        #print "file %s doesn't exist"%file
        return None
    pf.write(buf)
    pf.close()
    


def _getFileSE(file):
    if file.find('/') >= 0:
        return(file[0:file.rfind('/')])
    else:
        se=SEtools( MyConf().gLite.vo, MyConf().gLite.se)
        return(se._simpleDelTag(file))


def _getFile(file):
    if file.find('/') >= 0:
        return(file[0:file.rfind('/')])
    else:       
        return(file)

def _removeFinalSlash(name):
    if name[-1] == '/':
        return name[:-1]
    else:
        return name
    
def _getPath(file):
    if file.find('/') >= 0:
        return(file[file.rfind('/')+1:])
    else:       
        return('')

   
###############################################################
#
#  Tool class 
#
###############################################################

class ConfigFile:
    """Generic class parser """
    def __init__(self, file):
        self._file = file
        if not os.path.isfile(file):
            #sys.stderr.write("\nERROR: %s doesnt't exist !!\n"%file)   
            #sys.exit(1)
            return
        pFile = open(file, 'r')
        conf = pFile.read()
        pFile.close()        
        tabline = conf.split('\n')
        for line in tabline:
            words = line.split()
            if len(words) == 0:
                continue
            if words[0][0] == "#":
                continue
            if hasattr(self, words[0]):
                if len(words) == 1:
                    sys.stderr.write("\nERROR: in file %s, no value for argument '%s' !!\n"%(file, words[0]))
                    exit(1)
                myType = type(getattr(self, words[0]))
                if myType == types.StringType:
                    setattr(self, words[0], words[1])
                else:
                    try: 
                        setattr(self, words[0], (myType)(words[1]))
                    except ValueError:
                        sys.stderr.write("\nERROR: parameter '%s' in file %s must be %s !!\n"%(words[0],file, myType))
                        exit(1)
            else:
                sys.stdout.write("\nWARNING: unknown parameter '%s' in file conf %s\n"%(words[0],file))
        #print inspect.getmembers(self)


class ConfigGrid(ConfigFile):
    """Personnal Glite parameters"""
    _defaultFile = os.path.expanduser('~/.apcgrid/apcgrid.conf')
    def __init__(self):
        # define attribut and type
        self.file = ConfigGrid._defaultFile
        self.vo= ''
        self.lfchost = ''
        self.se = ''
        self.ce = ''       
        ConfigFile.__init__(self,self.file)
        os.environ['LCG_CATALOG_TYPE']='lfc'
        os.environ['LFC_HOST']=self.lfchost       
        if G_verbose>5:print "Fin INIT ConfigGrid"

    def setFileConf(self, file):
        ConfigGrid._defaultFile = file
        MyConf.instance = None
        if G_verbose >= 4: print "ConfigGrid: change file %s"%file

        
class ConfigPerso(ConfigFile):
    """Personnal information"""
    def __init__(self):
        self.file = os.path.expanduser('~/.apcgrid/apcperso.conf')
        self.mail = ''
        self.workDir = os.getcwd()+'/'
        ConfigFile.__init__(self,self.file )
        
        
class MyConf(object):
    """Personnal configuration for APCScheduler with design pattern Singleton"""
    class __Singleton:
        def __init__(self):
            self.gLite = ConfigGrid()
            self.info = ConfigPerso()
         
    instance = None

    def __new__(cls):
        if not MyConf.instance:
            MyConf.instance = MyConf.__Singleton()
        return MyConf.instance

    def __getattr__(self, attr):
        return getattr(self.instance, attr)

    def __setattr__(self, attr, val):
        return setattr(self.instance, attr, val)

#
# Storage Element management
#

class ObjFileSystem:
    def __init__(self, nameObj, fileSystem):
        self._name = nameObj
        self._fs = fileSystem        
        self._perm = None
        fileSystem.defType(self)        
    def getName(self): raise
    def size(self): raise
    def isFile(self): raise
    def isLink(self): raise
    def isDir(self): raise
    def isNone(self): raise

    
class FileFileSystem(ObjFileSystem):    
    def __init__(self, nameObj, fileSystem):
        ObjFileSystem.__init__(self, nameObj, fileSystem)
    def getName(self): return 'FILE'
    def size(self): return self._fs.sizeFile(self)
    def isFile(self): return True
    def isLink(self): return False
    def isDir(self): return False
    def isNone(self): return False

        
class NoneFileSystem(ObjFileSystem):    
    def __init__(self, nameObj, fileSystem):
        ObjFileSystem.__init__(self, nameObj, fileSystem)
    def getName(self): return 'NONE'
    def size(self): return 0
    def isFile(self): return False
    def isLink(self): return False
    def isDir(self): return False
    def isNone(self): return True

        
class LinkFileSystem(ObjFileSystem):    
    def __init__(self, nameObj, fileSystem):
        ObjFileSystem.__init__(self, nameObj, fileSystem)
    def getName(self): return 'LINK'
    def size(self): return 0
    def isFile(self): return False
    def isLink(self): return True
    def isDir(self): return False
    def isNone(self): return False


class DirFileSystem(ObjFileSystem):    
    def __init__(self, nameObj, fileSystem):
        ObjFileSystem.__init__(self, nameObj, fileSystem)
    def getName(self): return 'DIR'
    def size(self): return self._fs.sizeDir(self)
    def isFile(self): return False
    def isLink(self): return False
    def isDir(self): return True
    def isNone(self): return False


class FSystem:
    def _init(self, obj): pass
    def getName(self): return 'NOT DEFINE'
    def isDir(self,obj): raise
    def isFile(self,obj): raise
    def isLink(self,obj): raise
    def sizeFile(self): raise
    def sizeDir(self): raise
    def defType(self, obj):
        if self.isDir(obj) : obj.__class__ = DirFileSystem
        elif self.isLink(obj): obj.__class__ = LinkFileSystem
        elif self.isFile(obj): obj.__class__ = FileFileSystem
        else : obj.__class__ = NoneFileSystem

               
class FSystemSE(FSystem):
    def __init__(self, vo):
        self._vo = vo
    def _init(self, obj): self._getPerm(obj)
    def getName(self): return 'SE'
    def isDir(self,obj): return (obj._perm[0]=='d')
    def isFile(self,obj): return (obj._perm[0]=='-')
    def isLink(self,obj):  return (obj._perm[0]=='l')  
    def sizeFile(self,obj): return obj._size
    def sizeDir(self,obj):
        myp = Process("lfc-ls -l /grid/%s/%s | grep ^[^d] | awk '{s=s+$5} END {print s}'"%(self._vo, obj._name))
        myp.wait(30)
        if myp.getExitValue() != 0:
            sys.stderr.write(myp.stdErr()) 
            return -1
        elif len(myp.stdOut()) == 0:
            return 0
        else:
            if G_verbose>=15: 
                print "stdout='"+myp.stdOut()+"'"
            try:
                mys = int(myp.stdOut())
            except:
                mys = 0
            return mys
       
    def defType(self, obj):
        self._init(obj)        
        if obj._perm == None:
            obj.__class__ = NoneFileSystem
            return
        FSystem.defType(self,obj)
    
    
    def _getPerm(self, obj):
        # remove last
        path = obj._name
        if path=='':
            obj._perm = 'd'        
            obj._size = 0
            return True
        if path.find('/') >= 0:
            path_tmp = path[0:path.rfind('/')]
            name_tmp = path[path.rfind('/')+1:]            
        else:
            path_tmp = ''
            name_tmp = path
        if G_verbose>=10:
            print 'path_tmp='+path_tmp
            print 'name_tmp='+name_tmp
        mycmd = "lfc-ls -l /grid/%s/%s | grep %s"%(self._vo, path_tmp, name_tmp ) 
        myp = Process(mycmd)
        myp.wait(30)
        if myp.getExitValue() != 0:
            sys.stderr.write(myp.stdErr())            
        list = myp.stdOut().split('\n')        
        for dir in list:
            if dir == "" : continue
            mot = dir.split()
            if G_verbose>=10:
                print dir
                print mot            
            if mot[8] == name_tmp:
                if G_verbose>=10: print "find objet, permission is ", mot[0]
                obj._perm = mot[0]        
                obj._size = int(mot[4])

        
class FSystemUNIX(FSystem):
    def _init(self, obj): pass
    def getName(self): return 'UNIX'
    def isDir(self,obj):   return os.path.isdir(obj._name)
    def isFile(self,obj):  return os.path.isfile(obj._name)
    def isLink(self,obj):  return os.path.islink(obj._name)  
    def sizeFile(self,obj):return os.path.getsize(obj._name)
    def sizeDir(self,obj):
        pp = Process('ls -l %s'%obj._name)
        pp.wait(60)
        size = 0
        for line in pp.stdOut().split('\n'):               
            if line == '': continue
            if line[0:5] == "total": continue               
            if (line[0] == '-') : size += int(line.split()[4])
        return size

    
class SEtools:
    def __init__(self, NameVO, NameSE):
        self._LFC_HOST = os.getenv('LFC_HOST')
        if self._LFC_HOST == None:
            print "ERROR: Define LFC_HOST in your environnement"
            raise
        self._NameSE = NameSE
        self._NameVO = NameVO
        self._tag = "se:"
        self._SEis = None
        self._lentag = len(self._tag)
        self._cp = {'SRC': self._cp2Loc, 'DEST': self._cp2SE }
        self._cprec = {'SRC': self._cp2Locrec, 'DEST': self._cp2SErec }
        self._lscmd = {'SRC': self._lsSEfile,'DEST': self._lsLocfile }        
        self._mkdir = {'SRC': self._mkdirLoc,'DEST': self._mkdirSE }

    def _whereSE(self,src,dest):
        "File on storage element must begining by se:"
        if src[0:self._lentag] == self._tag:
            self._SEis = "SRC"           
        elif  dest[0:self._lentag] == self._tag:
            self._SEis = "DEST"
        else:
            print "ERROR: src or dest must begin by "+self._tag
            raise
        return self._SEis
     
    def _deltag(self,src,dest):
        if self._SEis == "SRC":
            return [src[self._lentag:],dest]
        else:
            return [src,dest[self._lentag:]]
            
    def _SubmitCmd(self, cmd, TpsMax=3600):
        myp = Process(cmd)       
        if G_verbose > 4: print cmd
        myp.wait(TpsMax)        
        if not myp.isOk():           
            myp.stdOut()
            myp.stdErr()
            sys.stderr.write("\nERROR: "+cmd+"\n")
            sys.stderr.write(myp.stdErr())
            sys.exit(1)            
        return [myp.stdOut(), myp.stdErr()]
        
    def _cp2SE(self , src, destSE):
        fSE = "lfn:/grid/"+self._NameVO+'/'+destSE
        fsrc= "file:"+src
        cmd = "lcg-cr  -n 3 --vo %s -d %s %s -l %s"%(self._NameVO, self._NameSE, fsrc, fSE)
        return submitCmd(cmd,60)
    
    def _cp2SErec(self , src, destSE, er, rec=False, nbstream=1):
        if rec :
            self._mkdirSE(destSE)
        else:
            self._cp2SE(src, destSE)
            return
        pp = Process('ls -l %s'%src)
        pp.wait(60)
        if pp.isOk():                
            task_File = MultiJobsClass()
            for line in pp.stdOut().split('\n'):               
                if line == '': continue
                if line[0:5] == "total": continue               
                if (line[0] == 'd') and rec :                    
                    dir = line.split()[8]
                    if G_verbose >=2:
                        sys.stdout.write("\ncpdir "+src+'/'+dir)
                        sys.stdout.flush()
                    MyExe = AppliExe('apcgrid-cp')
                    newdirSE= "%s/%s"%(destSE,dir)                   
                    arg = '-r -n %d %s/%s se:%s -v %d --ncd '%(nbstream,src, dir, newdirSE, G_verbose)
                    if er: arg = arg + ' -e "%s"'%er
                    MyExe.setArg(arg)
                    MyExe.noKeepStdFile()                   
                    job= newJob(MyExe,'LOCAL')
                    job.timerUpdateStatus("1")
                    job.submitAndWait()                    
                    if job.isOk() and G_verbose >=2:                       
                        sys.stdout.write(job.stdOut())
                        sys.stdout.flush()
                    if not job.isOk(): sys.stderr.write(job.stdErr())
                else:
                    nfile = line.split()[8]
                    if er:
                        if re.match(er, nfile) == None: continue
                    fSE = "lfn:/grid/"+self._NameVO+'/'+destSE+'/'+nfile
                    fsrc= "file:"+src+'/'+nfile
                    arg = " -n %d --vo %s -d %s %s -l %s"%(nbstream, self._NameVO, self._NameSE, fsrc, fSE)
                    if G_verbose >=4:
                        sys.stdout.write("\ncp "+src+'/'+nfile)
                        sys.stdout.flush()
                    MyExe = AppliExe('lcg-cr')
                    MyExe.setArg(arg)
                    MyExe.noKeepStdFile()
                    task_File.append(newJob(MyExe,'LOCAL'))                    
                                    
            task_File.timerUpdateStatus("1")
            task_File.submitAndWaitAll( MaxRunning=10)            
            for job in task_File._ListJob:                
                if job.isOk() and G_verbose >=2:
                    sys.stdout.write(job.stdOut())
                    sys.stdout.flush()
                if not job.isOk(): sys.stderr.write(job.stdErr())
        
    def _cp2Loc(self, srcSE, dest, ns=1):
        fSE = "lfn:/grid/"+self._NameVO+'/'+srcSE
        fdest= "file:"+dest
        #cmd = "lcg-cp -n %d --vo %s %s %s"%(ns, self._NameVO,fSE,fdest)
        print dest
        cmd = "lcg-cp --vo %s %s %s"%( self._NameVO,fSE,fdest)
        return submitCmd(cmd)
    
                
    def _lsSEfile(self, dir):
        """ ls uniquement les non fichiers """
        return "lfc-ls -l /grid/%s/%s| grep ^[^d]|awk '{print $9}'"%(self._NameVO, dir)        
                   
    def _lsLocfile(self, dir):
        return "ls -F --file-type %s |grep [^/]$ "%dir
    
    def _mkdirSE(self, dir):
        cmd = "lfc-mkdir  /grid/%s/%s"%(self._NameVO, dir)
        myp = Process(cmd)       
        myp.wait(120)
        if not myp.isOk():
            # maybe already exist ?
            myerr = myp.stdErr()
            if myerr.find('File exists')< 0:
                sys.stderr.write(myerr)
                myp.stdOut()
                raise
            
    def _mkdirLoc(self, dir):
        if not os.path.isdir(dir):
            os.makedirs(dir)
            
    def _tagSEpresent(self, file):
        if len(file) >=self._lentag:
            return file[0:self._lentag]==self._tag
        else:
            return False
         
    def _simpleDelTag(self, file):
        if self._tagSEpresent(file):
            return file[self._lentag:]
        else:
            return file
        
    def _rmrec(self,dir, er, checkdir=True, rec=False):
        dir = self._simpleDelTag(dir)
        if checkdir:           
            if not self.isDir(dir):            
                self.rm(dir)
                return            
        if er == "":
            # comme unix rm -r dir, si pas d'expression, le repertoire courant est supprime
            rmCurDir = True
            er = ".*"
        else:
            rmCurDir = False
        pp = Process('lfc-ls -l /grid/%s/%s'%(self._NameVO,dir))
        pp.wait(60)
        if pp.isOk():                
            task_rmFile =  MultiJobsClass()       
            task_rmDir =  MultiJobsClass()       
            for line in pp.stdOut().split('\n'):
                if line == '': break
                if (line[0] == 'd') and rec:
                    newdir =  dir+'/'+line.split()[8]
                    if G_verbose >=4:
                        sys.stdout.write("\nrm dir "+newdir)
                    MyExe = AppliExe('apcgrid-rm')                   
                    arg = '-r %s -v %d --ncd '%(newdir, G_verbose)
                    if er != ".*" :
                        # pour effacer les sous repertoires vides
                        arg = arg +' -e "%s"'%er
                    MyExe.setArg(arg)
                    MyExe.noKeepStdFile()
                    task_rmDir.append( newJob(MyExe,'LOCAL'))
                else:                   
                    nfile = line.split()[8]
                    if re.match(er, nfile) == None: continue
                    nfile = dir+'/'+nfile
                    arg  = "--vo %s -a lfn:/grid/%s/%s --force"%(self._NameVO, self._NameVO, nfile)              
                    if G_verbose >=4:
                        sys.stdout.write("\nrm %s"%nfile)
                    MyExe = AppliExe('lcg-del')
                    MyExe.setArg(arg)
                    MyExe.noKeepStdFile()
                    task_rmFile.append(newJob(MyExe,'LOCAL'))
                
            task_rmFile.timerUpdateStatus("1")            
            task_rmDir.timerUpdateStatus("1")              
                       
            task_rmFile.submitAndWaitAll( MaxRunning=10)           
            for job in task_rmFile._ListJob:              
                if job.isOk() and G_verbose >=2:   sys.stdout.write(job.stdOut())
                if not job.isOk():   sys.stderr.write(job.stdErr())
                    
            task_rmDir.submitAndWaitAll( MaxRunning=10)
            for job in task_rmDir._ListJob:               
                if job.isOk() and G_verbose >=2:   sys.stdout.write(job.stdOut())
                if not job.isOk():  sys.stderr.write(job.stdErr())
        if rmCurDir:
            # test si repertoire vide
            pp = Process('lfc-ls  /grid/%s/%s"'%(self._NameVO,dir))
            pp.wait(60)
            if not pp.isOk():
                sys.stderr.write(pp.stdErr())
            if pp.stdOut() == '':
                if G_verbose >=4: sys.stdout.write("\nrmdir se:"+dir)
                cmd = "lfc-rm -r /grid/%s/%s"%(self._NameVO,dir)
                self._SubmitCmd(cmd, 60)
                     
    def _cp2Locrec(self, srcSE, dest, er, rec=False, nbstream=1, pref=""):
        if rec:            
            self._mkdirLoc(dest)
        else:
            return self._cp2Loc(srcSE, dest,nbstream)
            
        if er == "": er = ".*"
        pp = Process('lfc-ls -l /grid/%s/%s'%(self._NameVO, srcSE))
        pp.wait(60)
        if pp.isOk():                
            task_File =  MultiJobsClass()
            for line in pp.stdOut().split('\n'):
                if line == '': break
                if (line[0] == 'd') and rec:
                    dir = line.split()[8]
                    newdirSE =  srcSE+'/'+dir
                    newdir = dest+'/'+pref+dir
                    if G_verbose >=4:
                        sys.stdout.write("\ncp dir "+newdirSE)
                    MyExe = AppliExe('apcgrid-cp')
                    arg = 'se:%s %s -r -v %d -n %d --ncd'%(newdirSE, newdir, G_verbose, nbstream)
                    if er: arg = arg + ' -e "%s"'%er
                    MyExe.setArg(arg)
                    if G_verbose >=4:
                        sys.stdout.write("\napcgrid-cp "+arg)                    
                    MyExe.noKeepStdFile()
                    job=newJob(MyExe,'LOCAL')
                    job.timerUpdateStatus("1")
                    job.submitAndWait()
                    if job.isOk() and G_verbose >=2:                       
                        sys.stdout.write(job.stdOut())
                        sys.stdout.flush()
                    if not job.isOk():  sys.stderr.write(job.stdErr())
                else:
                    nfile = line.split()[8]
                    if er :
                        if re.match(er, nfile) == None: continue
                    pfile = srcSE+'/'+nfile
                    if G_verbose >=4:
                        sys.stdout.write("\ncp %s"%pfile)
                    MyExe = AppliExe('lcg-cp')
                    fSE = "lfn:/grid/"+self._NameVO+'/'+pfile                    
                    fdest= "file:"+dest+"/"+pref+nfile
                    # -n bloque tout !!!!
                    #arg  = "-n %d --vo %s %s %s"%(nbstream, self._NameVO, fSE,fdest)
                    arg  = " --vo %s %s %s"%( self._NameVO, fSE, fdest)
                    if G_verbose >=4:
                        sys.stdout.write("\nlcg-cp "+arg)
                    MyExe.setArg(arg)
                    MyExe.noKeepStdFile()
                    task_File.append(newJob(MyExe,'LOCAL'))                   
                
            task_File.timerUpdateStatus("1")                       
            task_File.submitAndWaitAll( MaxRunning=10)           
            for job in task_File._ListJob:               
                if job.isOk() and G_verbose >=2:
                    sys.stdout.write(job.stdOut())
                    sys.stdout.flush()
                if not job.isOk():   sys.stderr.write(job.stdErr())
                    

    def _testProxy(self, stderr, dotest):
        if (stderr.find('credential')>0) and dotest:
            return( _initProxy()==0)                    
        return False


    #
    # PUBLIC
    #
    def cp(self,src,dest):
        "File on storage element must begining by se:"    
        self._whereSE(src,dest)
        a = self._deltag(src,dest)        
        return self._cp[self._SEis](a[0],a[1])
    
    def cprec(self, src, dest, re, rec, nbstream, check):
        """
        Action : copy recursif UI <-> SE
        File on storage element must begining by se:
        src
        dest
        re
        rec : flag copy recursif
        nbstream
        check
        """    
        self._whereSE(src,dest)
        a = self._deltag(src,dest) 
        a[0] = _removeFinalSlash(a[0])  
        a[1] = _removeFinalSlash(a[1])  
        if check:
            if G_verbose >=10: print "check cprec"
            if self._SEis == "SRC":
                objSrc  = ObjFileSystem(a[0], FSystemSE(self._NameVO))
                objDest = ObjFileSystem(a[1], FSystemUNIX())
            else:
                objSrc  = ObjFileSystem(a[0], FSystemUNIX())
                objDest = ObjFileSystem(a[1], FSystemSE(self._NameVO))
                
            if G_verbose >=10:
                print "objSrc.getName()", objSrc.getName()
                print "objDest.getName()",objDest.getName()
                print "rec", rec
                print "re", re              
                
            if rec or (re != None):
                # RECURSIF               
                if not objSrc.isDir():
                    sys.stderr.write("\nERROR 1: '%s' must be a directory."%a[0])
                    return 1
                if not objDest.isDir():
                    if not objDest.isNone():
                        sys.stderr.write("\nERROR 2: '%s' must be a directory."%a[1])
                        return 1
                    else:
                        # dest n'existe pas, est-t-il porter un repertoire ?
                        base = os.path.dirname(a[1])
                        if self._SEis == "SRC":
                            objBase = ObjFileSystem(base, FSystemUNIX())
                        else:
                            objBase = ObjFileSystem(base, FSystemSE(self._NameVO))
                        if not objBase.isDir():
                            sys.stderr.write("\nERROR 3: '%s' must be a directory."%base)
                            return 1                            
            else:
                # NON RECURSIF
                # src ne pas etre un repertoire                
                if objSrc.isDir():                    
                    sys.stderr.write("\nERROR: '%s' can't be a directory or used -r option"%a[0])
                    return 1
                if objSrc.isNone():
                    sys.stderr.write("\nERROR: '%s' unknown."%a[0])
                    return 1
                # dest name file present ?
                if objDest.isDir():
                    # dest est un repertoire ajout du nom du fichier source
                    a[1] = a[1] + '/'+os.path.basename(a[0])
                else:
                    # dest n'est pas un repertoire, verifions qu'il est porté par un repertoire valide
                    base = os.path.dirname(a[1])
                    if base == "": base = './'
                    if self._SEis == "SRC":             
                        objBase = ObjFileSystem(base, FSystemUNIX())
                    else:
                        objBase = ObjFileSystem(base, FSystemSE(self._NameVO))
                        print "objBase is ", objBase.getName()
                    if not objBase.isDir():
                        sys.stderr.write("\nERROR 4: '%s' must be a directory."%base)
                        return 1                                    
        return self._cprec[self._SEis](a[0],a[1], re, rec, nbstream)

        
    def cpdir(self,src,dest,prefixe=''):
        "Directory on storage element must begining by se:"
        # define where is SE direction src or dest
        SEis = self._whereSE(src,dest)
        (srcf, destf) = self._deltag(src,dest)

        # create directory
        self._mkdir[SEis](destf)

        # list file in src directory
        cmd  = self._lscmd[SEis](srcf)
        myp = Process(cmd)
        myp.wait(120)        
        if myp.isOk(): 
            list = self._SubmitCmd(cmd,60)[0].split()
                
            # copy loop file
            for myfile in list:
                self._cp[SEis](srcf+'/'+myfile, destf+'/'+prefixe+myfile)
        else:
            if G_verbose>=10:
                print "[SEtools.cpdir] Nothing to copy"

    def isDir(self,path):
        # remove last
        if path=='': return True
        if path.find('/') >= 0:
            path_tmp = path[0:path.rfind('/')]
            name_tmp = path[path.rfind('/')+1:]            
        else:
            path_tmp = ''
            name_tmp = self._simpleDelTag(path)
        if G_verbose>=15:
            print 'path_tmp='+path_tmp
            print 'name_tmp='+name_tmp           
        myp = Process("lfc-ls -l /grid/%s/%s | grep ^d| awk '{print $9}'"%(self._NameVO,self._simpleDelTag(path_tmp)))
        myp.wait(30)
        if myp.getExitValue() != 0:
            sys.stderr.write(myp.stdErr())
            return None
        listdir = myp.stdOut().split('\n')
        for dir in listdir: 
            if dir == name_tmp:
                return True
        return False
                
    def ls(self,dir="", retry=False):
        dir = self._simpleDelTag(dir)
        cmd = "lfc-ls -l /grid/%s/%s"%(self._NameVO,dir)
        if G_verbose>=15: print cmd
        myp = Process(cmd)
        myp.wait(30)
        if myp.getExitValue() != 0:
            sys.stderr.write("ERROR:\n")
            sys.stderr.write(myp.stdErr())           
            if self._testProxy(myp.stdErr(), retry):
                return self.ls(dir, False)                
        else:
            sys.stdout.write(myp.stdOut())           
        return myp.getExitValue()

    
    def sizeDir(self,dir):
        if not self.isDir(dir): return -1
        myp = Process("lfc-ls -l /grid/%s/%s | grep ^[^d] | awk '{s=s+$5} END {print s}'"%(self._NameVO, self._simpleDelTag(dir)))
        myp.wait(30)
        if myp.getExitValue() != 0:
            sys.stderr.write(myp.stdErr()) 
            return -1
        elif len(myp.stdOut()) == 0:
            return 0
        else:
            if G_verbose>=15: 
                print "stdout='"+myp.stdOut()+"'"
            try:
                mys = int(myp.stdOut())
            except:
                mys = -1
            return mys
            

    def sizeFile(self,file):
        if self.isDir(file) : return -1
        cmd = "lfc-ls -l /grid/%s/%s | grep ^[^d] | awk '{if (NR == 1) print $0}' | awk '{print $5}'"%(self._NameVO, self._simpleDelTag(file))
        myp = Process(cmd)
        myp.wait(30)
        if myp.getExitValue() != 0:
            sys.stderr.write(myp.stdErr()) 
            return -1
        elif len(myp.stdOut()) == 0:
            return -1
        else:
            if G_verbose>=15:
                print len(myp.stdOut())
                print "stdout='"+myp.stdOut()+"'"
            try:
                mys = int(myp.stdOut())
            except:
                mys = -1
            return mys


    def rm(self,file):
        cmd = "lcg-del --vo %s -a lfn:/grid/%s/%s --force"%(self._NameVO, self._NameVO, file)
        self._SubmitCmd(cmd, 60)


    def rmdir(self,dir):
        # list file in src directory
        cmd  = self._lsSEfile(dir)
        list = self._SubmitCmd(cmd,60)[0].split()
        
        # rm loop file
        for myfile in list:
            self.rm(dir+'/'+myfile)

        # del repertoire 
        cmd = "lfc-rm -r /grid/%s/%s"%(self._NameVO,dir)
        self._SubmitCmd(cmd, 60)            
 
        
class _CopySEThread(threading.Thread):
    MaxThread =  10
    listThread = []
    lockStaticVar = threading.Lock()
    
    def __init__(self, objCopy, pathdir, PathLocWD, prefixe ):
        threading.Thread.__init__(self)
        self.obj = objCopy
        self.pathdir = pathdir
        self.PathLocWD = PathLocWD
        self.pref = prefixe        
        while self.TooManyThread(): time.sleep(1)            
        _CopySEThread.lockStaticVar.acquire()
        _CopySEThread.listThread.append(self)
        _CopySEThread.lockStaticVar.release()
        
    def TooManyThread(self):
        _CopySEThread.lockStaticVar.acquire()
        #print "CopySEThread: ", len(_CopySEThread.listThread)
        test = len(_CopySEThread.listThread) > _CopySEThread.MaxThread
        _CopySEThread.lockStaticVar.release()
        return test
    
    def run(self):
        # copy from SE
        self.obj._cp2Locrec(self.pathdir+'/data', self.PathLocWD, "", True, 1, self.pref)
        # rm SE tempory directory
        mp = Process('apcgrid-rm -r '+self.pathdir)
        mp.wait()
        # free resource
        _CopySEThread.lockStaticVar.acquire()
        _CopySEThread.listThread.remove(self)
        _CopySEThread.lockStaticVar.release()
        # set flag finish
        MultiJobsClass.S_EventFinish.set()

    
class SuperviseProcess(threading.Thread):
    """Thread to supervise a process"""
    def __init__(self, ObjProcess):
        threading.Thread.__init__(self)
        self.Finish = False
        self.Process = ObjProcess

    def run(self):
        # method call by thread
        # wait process
        self.Process._wait()        
        self.Finish = True


class Process(popen2.Popen3):
    #S_nbInstance = 0
    """Safe process method wait() no """
    def __init__(self, cmd, stdout='' , stderr=''):
        self._cmd = cmd
        #Process.S_nbInstance +=1
        #if G_verbose>=15:        
        #print "\nAdd process ", cmd, Process.S_nbInstance

        nameAlea = os.getcwd()+"/"+_AddAlea("APCprocess",7)
        if stdout == '':
            self._rmStdOut = True
            self._nameStdOut = nameAlea+".out"
        else:
            self._rmStdOut = False
            self._nameStdOut = stdout
            
        if stderr == '':
            self._rmStdErr = True
            self._nameStdErr = nameAlea+".err"
        else:
            self._rmStdErr = False
            self._nameStdErr = stderr
            
        cmd_add = cmd + " 1>%s 2>%s"%(self._nameStdOut, self._nameStdErr)
        # appel au consytructeur de base    
        popen2.Popen3.__init__(self, cmd_add, True)       
        self._Status = 'SubmitRunning'        
        self._ret = None
        self._exitValue = None
        self._readErrFlag = True
        self._readOutFlag = True
        
#    def __del__(self):
#        Process.S_nbInstance -=1
#        if G_verbose>=15:    
#            print "\ndel process ", self._cmd, Process.S_nbInstance
        
    def _ExaminePoll(self):
        """Call when process finish"""        
        if self._exitValue != None: return
        self._exitValue = self.poll()>>8        
        if self._exitValue > 127:
            self._exitValue -= 256
        if G_verbose>=15:print "exit value "+str( self._exitValue)
        if self._exitValue == 0:
            self._Status = "FinishOK"
        else:        
            self._Status = "FinishNOK"
        self._retrieveOut()
        self._retrieveErr()
        self.fromchild.close()
        self.tochild.close()
        self.childerr.close()
                
                   
    def _updateStatus(self):
        """update status, no return"""        
        if self._Status.find('Finish') >= 0:
            # le process est fini pas de changement
            return
        cmd = 'ps -p %d -o state'%self.pid
        (o,i,e) = popen2.popen3(cmd)
        output = o.read()
        o.close()
        i.close()
        e.close()
        if G_verbose>=15 : print cmd+'\n'+output
        rep = output.split('\n')
        rep.remove('')
        if len(rep) >= 2:
            if (rep[1] in ['Z','X']):
                # le process est fini
                self._ExaminePoll()
            else:
                # toujours run
                pass
        else:
            # le process n'existe plus, il est termine
            self._ExaminePoll()
            
    def isAlive(self):
        self._updateStatus()
        return (self._Status == 'SubmitRunning')

    def isFinish(self):
        self._updateStatus()
        return (self._Status.find('Finish') >= 0)
    
    def isOk(self):
        self._updateStatus()
        return (self._Status == 'FinishOK')
    
    def _wait(self):
        return popen2.Popen3.wait(self)
    
    def wait(self,TimeOut=-1):
        if TimeOut == -1:
            # wait no limit 
            try:                           
                self._ret = self._wait()
            except:
                # process (certainement) termine avant d'arriver a ce wait
                pass
            self._ExaminePoll()         
        else:
            # wait with time out and kill if no reponse
            t = SuperviseProcess(self)
            t.start()
            t.join(TimeOut)           
            if not t.Finish:
                os.system("kill -9 %d"%self.pid)
                self._Status = "FinishKill"
                self._ret =  None # return 9                 
                if G_verbose>=15:print "Time out, kill process"
            else:                
                self._ExaminePoll()        
        self._retrieveOut()
        self._retrieveErr()
                

    def _readFile(self, nameFile):
        #print "_readFile", nameFile
        try:
            fo = open(nameFile,'r')
        except IOError:                      
            return '' 
        out = fo.read()
        fo.close()
        return out 
        
    def _retrieveOut(self):
        if self._rmStdOut:
            # read one time and keep result
            if self._readOutFlag:
                self._readOutFlag = False
                self._out = self._readFile(self._nameStdOut)
                os.system("rm -rf "+self._nameStdOut)
             
    def _retrieveErr(self):
        if self._rmStdErr:
            # read one time and keep result
            if self._readErrFlag:
                self._readErrFlag = False
                self._err = self._readFile(self._nameStdErr)
                os.system("rm -rf "+self._nameStdErr)
            
    def stdOut(self):
        if self.isFinish():
            if self._rmStdOut:
                return self._out
            else:
                return self._readFile(self._nameStdOut) 
        else:
            return None
        
    def stdErr(self):
        if self.isFinish():           
            if self._rmStdErr:
                return self._err
            else:
                return self._readFile(self._nameStdErr) 
        else:
            return None

    def getExitValue(self):
        return self._exitValue

    def getStatus(self):
        self._updateStatus()
        return self._Status


class GroupProcess(Process):
    """ si le nombre de process running est superieur a la limite :
          - si parametre Wait est False alors le process echoue
          - sinon attente qu'un process du groupe si termine pour le lancer 
    """   
    MaxProcess =  10
    listProcess = []
    def __init__(self, cmd, stdout='' , stderr='', Wait=True):
        if len(GroupProcess.listProcess) < GroupProcess.MaxProcess:
            Process.__init__(self,cmd,stdout, stderr)
            GroupProcess.listProcess.append(self)           
            return
        else:
            if not Wait:
                self._Status = "FinishSubmitNOK"
                self._exitValue = -1
                return
            while True:
                for pp in GroupProcess.listProcess:
                    if pp.isFinish():
                        Process.__init__(self,cmd, stdout, stderr)
                        GroupProcess.listProcess.append(self)
                        return
                #print "GroupProcess: wait end process to submit ", cmd
                time.sleep(1)
    
    def _updateStatus(self):
        if self._Status != "FinishSubmitNOK":
            Process._updateStatus(self)
            if self._Status.find('Finish') >= 0:
                #print "_updateStatus finish: ", self._cmd
                try:
                    GroupProcess.listProcess.remove(self)
                except:
                    pass


class _ProcessSubWMS(Process):
    """
    Reparti et regule les process submit Glite sur les WMS disponibles de la VO
    """
    # Variable static
    SlistProcess = [] # liste des ref. des Process submit
    Squeue = {}       # [wmsidx,[max,nbProcess]]

    # static method
    def _checkSlotFree(setIdx):
        listWMS=[]
        WMSpossible = False
        for key, value in _ProcessSubWMS.Squeue.items():
            if not set([key]).issubset(setIdx) and value[0] > 0:
                # we can used this WMS
                WMSpossible = True
                if value[0] > value[1]:
                    # a slot is free for this WMS
                    listWMS.append((value[1], key))
        if listWMS==[]:
            return [WMSpossible, None]
        else:
            listWMS.sort(key=operator.itemgetter(0))
            #print "listWMS ", listWMS
            return [True, listWMS[0][1]]
    _checkSlotFree = staticmethod(_checkSlotFree)


    def __init__(self, cmd, queueWMS, stdout='', stderr=''):
        if not _ProcessSubWMS.Squeue.has_key(queueWMS):
            sys.stderr.write("\nERROR:\n ProcessSubWMS unknow queueWMS %d\n"%queueWMS)
            return
        self._queueWMS = queueWMS
        # ajoute un process a la queue 'queueWMS'
        _ProcessSubWMS.Squeue[queueWMS][1] += 1
        # ajoute la reference du process à la liste generale
        _ProcessSubWMS.SlistProcess.append(self)
        Process.__init__(self, cmd, stdout, stderr)        

    
    def _updateStatus(self):
        # appel de la methode de la classe de base      
        Process._updateStatus(self)        
        if self._Status.find('Finish') >= 0:
            if self in _ProcessSubWMS.SlistProcess:
                #print "ProcessSubWMS: remove Process ", self._cmd
                _ProcessSubWMS.Squeue[self._queueWMS][1] -= 1
                _ProcessSubWMS.SlistProcess.remove(self)
    

###############################################################
#
#  Scheduler Interface
#
###############################################################

class SchedulerAbstract:
    def __init__(self):
        self._TypeSched =''         
        self._delegateProxy = "No"
        
    def cancel(self, Appli):
        """cancel Application"""        
        pass
    
    def submit(self, Appli):
        """submit Appli"""
        pass

    def delegateProxy(self):
        self._delegateProxy = "ToDo"
        
    def status(self, Appli):
        """Update status attribut of Appli object"""
        # Only test time out        
        if (Appli._Status.find('SubmitScheduled')==0):
            # Test Time out           
            TimeToSubmit = time.time() - Appli._TimeStart
            Grace = Appli._TimeOutToStart -TimeToSubmit 
            if G_verbose>5:print '\nTimeToSubmit=',TimeToSubmit,'Grace= ',Grace
            if Grace < 0:
                print 'Time Out !!!!!!'
                self.cancel(Appli)
                if not self._ReSubmit(Appli):
                    Appli._Status = 'FinishTimeOut'
                    
        if Appli._Status.find('Finish') >= 0:
            try:      
                del JobClass.dictJobSubmit[Appli._APCSchedID]
            except:
                pass
            if (Appli._Status.find('FinishOK') >= 0 or Appli._Status.find('FinishNOK') >= 0):            
                self.retrieveOutput(Appli)
                if Appli._Status.find('FinishOK') >= 0 :
                    self._cleaner(Appli)
                    Appli._Cleaner()                            
                            
    def retrieveOutput(self, Appli):
        """Retrieve Appli outputs in work directory"""
        pass
    
    def wait(self, Appli):
        """wait the end of Appli"""
        if Appli._Status.find('Submit') != 0:
            return
        self.status(Appli)
        while Appli._Status.find('Submit') == 0:
            time.sleep(Appli._Timer)
            self.status(Appli)
            #print "After wait %s new status is %s"%(Appli._APCSchedID, Appli._Status)


    def _ReSubmit(self, Appli):
        """reSubmit Appli"""
        Appli._FlagResubmit = True
        
    def _cleaner(self, Appli):
        """scheduler cleaner after end of job """
        pass
    
    def _resultListJob(self, listJob):
        return ""

    def stdOut(self, Appli):
        if  Appli._keepStdFile:
            return _readFile(Appli.getPathFile('STDOUT'))
        else:
            return  Appli._stdOut

    def stdErr(self, Appli):
        if  Appli._keepStdFile:
            return _readFile(Appli.getPathFile('STDERR'))
        else:
            return  Appli._stdErr
       
    
    
                   
##################################################
#    
# GENERIC GRID 
#

        
class SchedulerGrid(SchedulerAbstract):
    # Variable static a la classe
    DoProxyTest = True   
    S_DelegateProxy = False   
    def __init__(self):
        SchedulerAbstract.__init__(self)
        self._FileConf = ''
        self._FileJDL = ''
        self._pFile = ''       
        self._Req = 'Requirements = other.GlueCEStateStatus == "Production" && ( ! ( RegExp(".*node16.*",other.CEId) ) );'
        self._LoadBalCE = None
        self._defCE = None
                
    def _FillJDL(self):
        pass

    def _FillConf(self):
        pass

    def submitCmd(self, FileID):
        pass
    
    def statusCmd(self, FileID):
        pass
    
    def cancel(self, Appli):        
        cmd = self._CancelCmd(Appli)
        if G_verbose>=10: print "cancel job: "+cmd
        pp = Process(cmd)
        Appli._Cleaner()
        pp.wait(30)
        if not pp.isOk():
            sys.stderr.write("\nERROR:\n commend '%s' is %s\n"%pp._cmd, pp.getStatus())
            sys.stderr.write(pp.stdErr())
        else:
            sys.stdout.write(pp.stdOut())
        
    def _CancelCmd(self, Appli):
        pass
    
    def retrieveCmd(self, FileID, Appli, outDir):
        pass

    def doFileConf(self, wms):       
        self._pFile = open(self._FileConf, 'w+')
        self._FillConf(wms)
        self._pFile.close() 

    def doFileJDL(self, Appli):
        # 1- Application part
        buffer = Appli._FillFileJDL()

        # 2- job part
        buffer += 'StdOutput  = "%s";\n'%Appli.getNameStdOut()
        buffer += 'StdError   = "%s";\n'%Appli.getNameStdErr()
        
        outtemp = 'OutputSandbox = {"%s","%s"'%(Appli.getNameStdOut(),Appli.getNameStdErr())
        for elt in Appli._ListOutput:
            # add unique name executable
            #outtemp += ',"%s_%s"'%(Appli._APCSchedID, elt)
            if elt.find("lfn:/grid") == -1:
                # Don't add file se in OutputSandbox
                outtemp += ',"%s"'%elt
        outtemp += '};\n'
        buffer += outtemp

        # 3- scheduler part         
        self._pFile = open(self._FileJDL, 'w+')
        # renouvellement du proxy
        #buffer += "MyProxyServer = `myproxy.grif.fr`;"
        print >> self._pFile,'%s'%buffer
        Appli._ce = self._FillJDL(Appli)
        self._pFile.close()       

    def checkProxy(self): 
        """        
        """       
        cmd = 'glite-voms-proxy-info'
        myp = Process(cmd)
        myp.wait(5)
        output =  myp.stdOut()
        err  =  myp.stdErr()
        if G_verbose>5:print cmd                          
        if G_verbose>9: print output         
        if G_verbose>9: print err
        minHourProxyTimeOut = 48
        initProxy =False        
        if myp.isOk():
            timeleft_hhmmss = _GetWordsAfter(output,'timeleft  :')
            timeleft_ss = _ConvHHMMSSInSec(timeleft_hhmmss)
            if G_verbose>=5:
                print 'proxy timeleft: ',timeleft_hhmmss,timeleft_ss        
            if int(timeleft_ss) < 60*60*minHourProxyTimeOut:
                sys.stdout.write("\nProxy valid but time life proxy too short < %d hours."%minHourProxyTimeOut)
                initProxy = True
            else:
                if G_verbose>=2: print 'Proxy OK'           
                SchedulerGrid.DoProxyTest = False
        else:        
            initProxy = True

        if initProxy:
            nb_hours= 3*24              
            try:
                hours=input("\nProxy initialisation, enter duration in hours and I add %d hours or [Enter] to pass: "%minHourProxyTimeOut)
                try:
                    nb_hours = max(minHourProxyTimeOut+hours, minHourProxyTimeOut)                    
                except:
                    sys.stderr.write("\n%s is not a number, fix duration for 3 days\n")
                    nb_hours= 3*24
            except:
                # si je ne peux saisir le nombre de jours
                # alors cela sera pareil pour la pass phrase du proxy
                # on continue quand meme ...
                sys.stderr.write("\nCan't initialisation proxy no interactive mode !\n")
                SchedulerGrid.DoProxyTest = False 
                return                
            #Rem
            #La vérification de la prise ne compte de _initProxy est fait 
            #en rappelant la methode checkProxy()          
            _initProxy(nb_hours)

            
    def loadBalancingCE(self, vers, query):
        pass
    
    def _initDelegateProxy(self):
        ret = os.system('myproxy-init -d -n')
        time.sleep(2)
        if G_verbose >=10:
            print "status myproxy-init is ", ret         
        return ret
            
    def submit(self, Appli):            
        if self._delegateProxy == "ToDo":
            if not SchedulerGrid.S_DelegateProxy:
                self._initDelegateProxy()
            self._delegateProxy = "Yes"
                        
        prefixe = Appli._PathLocWD + Appli._APCSchedID
        self._FileJDL = prefixe+'.jdl'
        self._FileConf= prefixe+'.conf'         
        self.doFileJDL(Appli)
        Appli._wmsIdx = self._GetWMS(Appli._wmsUsed)
        if Appli._wmsIdx == None:
            # any WMS available            
            Appli._Status = 'FinishSubmitNOK'
            if len(Appli._wmsUsed)==0:
                sys.stderr.write("\nERROR:\n _GetWMS any wms available\n")
            del JobClass.dictJobSubmit[Appli._APCSchedID]
            return
        Appli._wmsUsed=Appli._wmsUsed.union(set([Appli._wmsIdx]))
        self.doFileConf(Appli._wmsIdx)            
        cmd = self.submitCmd(Appli)        
        Appli._ProcessSubmit = _ProcessSubWMS(cmd, Appli._wmsIdx)
        Appli._Status = "SubmitQuery"
        if G_verbose >=2:            
            mes='\nTry submit ' +Appli._APCSchedID+ ' on wms '+self._shortWMSname(Appli)
            sys.stdout.write(mes)

        
    def _AfterSubmit(self, Appli):
        output = Appli._ProcessSubmit.stdOut()
        error_sub  =  Appli._ProcessSubmit.stdErr()
        if G_verbose>=15:
            print 'submit output:'+output+'\nsubmit erreur:'+error_sub       

        # to do check submit success
        if  output.find('successfully submitted') >= 0:
            Appli._Status = 'SubmitScheduled'
            Appli._TimeStart = time.time()
            if G_verbose>=1:
                mes='\nSubmit ' +Appli._APCSchedID+ ' on wms '+self._shortWMSname(Appli)+' is ok.'                
                mes += "Used CE "+Appli._ce
                sys.stdout.write(mes)
            # extract ID grid
            pID = open(Appli._FileID, 'r')
            a = pID.read()
            Appli._IDgrid = a.split('\n')[1]
            pID.close()
            if G_verbose>=15:print 'ID grid: '+Appli._IDgrid
        else:
            if G_verbose>=10:
                print 'submit NOK\nsubmit output:'+output+'\nsubmit erreur:'+error_sub
            if G_verbose>=1:
                print '\nSubmit ' +Appli._APCSchedID+ ' is NOK on wms', self._shortWMSname(Appli)
            _writeFile(Appli.getPathFile('SUBERR'), error_sub)
            self._ReSubmit(Appli)
                            
            
    def status(self, Appli):        
        if Appli._Status.find('Submit') != 0:
            if G_verbose>10: print "status can't change !!!"
            return
        
        if Appli._Status == "SubmitQuery":
            if Appli._ProcessSubmit.isFinish():
                self._AfterSubmit(Appli)
                if Appli._Status == "SubmitQuery":
                    return
                Appli._ProcessSubmit = None
                # Si on n'a plus la possibilité de resoumettre
                # le job est declare fini
                if Appli.isFinish(): return 
            else:
                return
            
        if Appli._Status == "SubmitRetrieveSE":
            if not Appli._threadCopy.isAlive():
                if Appli._StatusAppli==0:
                    Appli._Status = "FinishOK"
                else:
                    Appli._Status = "FinishNOK"
                if G_verbose>=1:
                    mes='\n'+Appli._APCSchedID +' retrieve finish.'
                    sys.stdout.write(mes)
            return
        
                 
        if os.path.isfile(Appli._FileID):
            cmd = self.statusCmd(Appli._FileID) 
            myp = Process(cmd)    
            myp.wait()
            ouput = myp.stdOut()
            
            if G_verbose>9: print ouput
            status = _GetWordsAfter(ouput,'Current Status:')
            if status == None:
                Appli._Status = 'FinishNOKLostStatus'
            else:
                if G_verbose>=1:
                    mes='\n'+Appli._APCSchedID +' status is ' + str(status)
                    if G_verbose>=2:
                        mes+='.\t[WMS: %s\tCE: %s]'%(self._shortWMSname(Appli).split('.')[0], Appli._ce)
                    sys.stdout.write(mes)                    
                if (status.find('Running')>= 0):
                    Appli._Status = 'SubmitRunning'
                elif (status.find('Done')>= 0):                    
                    exitcode = _GetWordsAfter(ouput,'Exit code:')
                    if exitcode != None:
                        try:
                            Appli._StatusAppli = int(exitcode)
                            if Appli._StatusAppli == 0:
                                Appli._Status = 'FinishOK'
                            else:
                                Appli._Status = 'FinishNOK'
                        except:                            
                            Appli._Status = 'FinishOK'
                    else:                        
                        Appli._Status = 'FinishOK'                
                elif status.find('Cleared')>= 0:
                    Appli._Status = 'FinishUnknow'
                elif status.find('Cancelled')>= 0:
                    Appli._Status = 'FinishCancelled'
                elif status.find('Aborted')>= 0:
                    if not self._ReSubmit(Appli):
                        Appli._Status = 'FinishAborted'
                        _writeFile(Appli.getPathFile('STDERR'), ouput)
        else:
            print "FileID doesn't exist", Appli._FileID
            # temporary           
            Appli._Status = 'FinishLostIDgrid'
            
        # manage time out and call retrieve output
        SchedulerAbstract.status(self,Appli)
        if Appli._threadCopy != None:
            Appli._Status = "SubmitRetrieveSE"

            
    def retrieveOutput(self, Appli):        
        cmd, outpath = self.retrieveCmd(Appli._FileID, Appli)
        if G_verbose>9:
            print cmd, outpath
        myp = Process(cmd)
        myp.wait(360)
        if G_verbose>9:
            print myp.stdOut()
            print myp.stdErr()
        # used output directory indicated in output message
        retrieveStatus = False
        if os.path.isdir(outpath) and myp.isOk():
            outpath_glite = _GetWordsAfter(myp.stdOut(),'stored in the directory:')
            if outpath_glite != None:
                retrieveStatus = True
                # stdout            
                os.system('mv %s/%s %s'%(outpath_glite, Appli.getNameStdOut(), Appli._PathLocWD))

                # stderr
                os.system('mv %s/%s %s'%(outpath_glite, Appli.getNameStdErr(), Appli._PathLocWD))

                # output sandbox
                for i in range(len(Appli._ListOutput)):
                    RetFile = "%s/%s"%(outpath_glite, Appli._ListOutput[i])
                    if os.path.isfile(RetFile):
                        cmd = 'mv %s %s'%(RetFile, Appli.getPathFile("OUT",i))
                        os.system(cmd)
                    else:
                        print "Can't retrieve %s , doesn't exist!!"%RetFile

                # remove directory            
                os.system('rm -rf '+ outpath)

                # output SE
                for i in range(len(Appli._ListOutputSE)):                
                    cmd = 'lcg-cp --vo %s %s file:%s'%(MyConf().gLite.vo, Appli._ListOutputSE[i], Appli.getPathFile("OUT_SE",i))
                    if G_verbose > 5: print cmd
                    os.system(cmd)
            
        if not retrieveStatus:        
            print "============retrieveOutput : pb"
            print myp.stdOut()
            print myp.stdErr()     
            print "\n%s\n%s\n%s "%(outpath, cmd,  myp._Status)
            print "==============================="
            Appli._Status = 'FinishNOKretrievePB'
            return
        
        Appli._RetrieveFromSE()

    def _ReSubmit(self, Appli):
        SchedulerAbstract._ReSubmit(self, Appli)
        if G_verbose>8:print "Try _ReSubmit"
        cmd = 'rm -rf %s'%Appli._FileID                 
        os.system(cmd)
        Appli._Status='NotSubmit'
        self.submit(Appli)
        return Appli._Status != 'FinishSubmitNOK'
   
    def setCE(self, CEName):
        if self._defCE != None:
            sys.stderr.write("\nWARNING: CE already definied, conflict between load balancing and setCE ?")
            return        
        cmd = "lcg-infosites --vo %s ce | grep %s"%(MyConf().gLite.vo,CEName )
        myp = Process(cmd)
        myp.wait(100)
        if not myp.isOk():           
            print "\nout:\n"+myp.stdOut()
            print "\nerr:\n"+myp.stdErr()
            sys.stderr.write("ERROR with command  "+cmd)
            sys.stderr.write(myp.stdErr())
            sys.exit(1)
        out = myp.stdOut()
        if out == "":
            sys.stderr.write("\nERROR: unknown CE for your VO\nMy check procedure:\n %s "%cmd)
            exit(1)
        lineout = out.split('\n')
        splitout = lineout[0].split()
        if len(splitout) >=6 :
            if splitout[5].find(CEName)== 0:
                self._Req = 'Requirements = other.GlueCEUniqueID == "%s";'%splitout[5]
                self._defCE = splitout[5]
            else:
                sys.stderr.write("\nERROR: I don't exactly find your CE '%s' in \n%s\nresult:\n%s "%(CEName, cmd, out))
                sys.exit(1)             
        else:
            sys.stderr.write("\nERROR: invalid format lcg-infosites --vo xx ce, wait for 6 columns:\n%s"%out)
            sys.exit(1)
        
            
    def setLocalCE(self):
        return self.setCE(MyConf().gLite.ce)

                
    def _cleaner(self, Appli):
        """scheduler cleaner after end of job """        
        if Appli.isOk():
            file= os.path.join(Appli._PathLocWD,Appli._APCSchedID) 
            os.system("rm -rf "+file+".jdl")
            os.system("rm -rf "+file+".conf")

    def _resultListJob(self, listJob):
        # creation fichier avec tous les ID
        if listJob._name =="":
            filetemp = listJob._ListJob[0].addFullPath(_AddAlea("",6))
        else:
            filetemp = listJob._ListJob[0].addFullPath(_AddAlea(listJob._name.replace(' ','_') ,6))
        filetemp = filetemp+'.ids'        
        listJob.concatId(filetemp)

        # command gstat_all
        cmd = self._resultCmd(filetemp)
        lce=Process(cmd)
        lce.wait(60)
        if not lce.isOk():
            sys.stdout.write(lce.stdOut())
            sys.stderr.write("ERROR: "+lce.stdErr())
        buffer = "\nCompute Element used:\n---------------------\n"
        return buffer+lce.stdOut()
        
        
# GLITE
#

class SchedulerGLITE(SchedulerGrid):    
    ListWMS = []
    
    def __init__(self):
        SchedulerGrid.__init__(self)
        if not os.path.isfile(MyConf().gLite.file):
            sys.stderr.write("ERROR: file glite '%s' configuration doesn't exist.\nDo apcgrid-init to define it !!\n"%MyConf().gLite.file)
            sys.exit(1)
        self._VO= 'VirtualOrganisation = "%s";'%MyConf().gLite.vo        
        self._MyProxy= 'MyProxyServer       = "%s";'%os.getenv('MYPROXY_SERVER')
        self._Rank = 'Rank = ( other.GlueCEStateWaitingJobs == 0 ) ? ((other.GlueCEStateFreeCPUs==0)?-2222:other.GlueCEStateFreeCPUs) : - other.GlueCEStateWaitingJobs * 10 / (other.GlueCEStateRunningJobs + 1) * ( (other.GlueCEStateFreeCPUs == 0)?500:1 ) ;'    
        self._ForceWMS = -1
        self._TypeSched = 'GLITE'
        self._checkProxy()
        self._InitListWMS()

        
    def _checkProxy(self):
        if SchedulerGrid.DoProxyTest :           
            self.checkProxy()
            if SchedulerGrid.DoProxyTest :
                self.checkProxy()
                if SchedulerGrid.DoProxyTest:
                    print "ERROR: Proxy Failed"
                    sys.exit(1)
                   
    def setWMS(self, listWMSName):
        for idx in range(len(SchedulerGLITE.ListWMS)):
            # max 2 process submit on same WMS
            # Init at 0 process in course
            _ProcessSubWMS.Squeue[idx] = [0, 0]
       
        for ask_wms in listWMSName:
            findwms = False
            for idx in range(len(SchedulerGLITE.ListWMS)):        
                if SchedulerGLITE.ListWMS[idx].find(ask_wms) >= 0:
                    _ProcessSubWMS.Squeue[idx] = [2, 0]
                    findwms = True
                    break
            if not findwms:
                sys.stderr.write("\nERROR: your WMS %s isn't available for this VO %s"%(ask_wms, MyConf().gLite.vo))
                sys.exit(1)
                
    def excludeWMS(self, listWMS):
        for ask_wms in listWMS:
            findwms = False
            for idx in range(len(SchedulerGLITE.ListWMS)):        
                if SchedulerGLITE.ListWMS[idx].find(ask_wms) >= 0:
                    _ProcessSubWMS.Squeue[idx] = [0, 0]
                    findwms = True
                    break
            if not findwms:
                sys.stderr.write("\nERROR: your WMS %s isn't available for this VO %s"%(ask_wms, MyConf().gLite.vo))
                sys.exit(1)
      
    def _InitListWMS(self):
        """Private: Initialisation WMS List"""        
        if len(SchedulerGLITE.ListWMS) > 0 :
            return
        
        cmd = "lcg-infosites --vo %s wms"%MyConf().gLite.vo
        if G_verbose>=5:print cmd
        MyProcess = Process(cmd)
        MyProcess.wait()
        output = MyProcess.stdOut()    
        erreur = MyProcess.stdErr()        
        RetCode = MyProcess.getExitValue()
        if G_verbose>=5:print "output: \n"+ output
        if G_verbose>=5:print "erreur: \n"+ erreur
        if G_verbose>=5:print "Code de retour: " + str(RetCode)

        if RetCode != 0:
            sys.stderr.write("ERROR get WMS list: "+cmd)
            sys.stderr.write(erreur)
            sys.exit(1)
        List = output.split('\n')
        List.remove('')
        if len(List) == 0:
            sys.stderr.write("ERROR: Any WMS avalaible for VO %s !"%MyConf().gLite.vo)
            sys.exit(1)
                
        SchedulerGLITE.ListWMS = _ListUnique(List)
        if G_verbose>=5:
            for wms in SchedulerGLITE.ListWMS:                
                sys.stdout.write("\n"+wms)

        # Init wms queue of _ProcessSubWMS class, 1 WMS , 1 queue        
        for idx in range(len(SchedulerGLITE.ListWMS)):
            # max 2 process submit on same WMS
            # Init at 0 process in course
            _ProcessSubWMS.Squeue[idx] = [2, 0]
        #print _ProcessSubWMS.Squeue
        
        
    def _RemoveWMS(self, wmsIdx):
        # mise à 0 du max process submit
        _ProcessSubWMS.Squeue[wmsIdx][0] = 0


    def _GetWMS(self, setIdx):
        if len(SchedulerGLITE.ListWMS) == 0 :
            sys.stderr.write("\nERROR:any WMS available\n" );
            return None
        res = _ProcessSubWMS._checkSlotFree(setIdx)       
        if not res[0]:
            return None        
        # wait a free WMS
        while (res[1] == None):
            time.sleep(0.5)
            for pp in _ProcessSubWMS.SlistProcess: pp.isFinish()
            res = _ProcessSubWMS._checkSlotFree(setIdx)
            #print res
        if G_verbose>=5:
            sys.stderr.write("\nfree WMS :"+  SchedulerGLITE.ListWMS[res[1]])
            sys.stdout.write("\nnb process submit :"+  str(len(_ProcessSubWMS.SlistProcess)))
        return res[1]
            
        
    def _NameWMS(self, Appli):
        return SchedulerGLITE.ListWMS[Appli._wmsIdx]

    def _splitWMS(self, wms):
        return wms.split('/')[2].split(':')[0]
    
    def _shortWMSname(self, Appli):
        wms = self._NameWMS(Appli)
        return self._splitWMS(wms)
    
    def initWithRB(self, wmsIdx):
        wms = SchedulerGLITE.ListWMS[wmsIdx]
        self._NSAd= 'NSAddresses         = "%s:7772";'%self._splitWMS(wms)
        self._LBAd= 'LBAddresses         = "%s:9000";'%self._splitWMS(wms)
        self._WMProxy= 'WMProxyEndpoints    = {"%s"};'%wms

    def _FillConf(self,wms):
        self.initWithRB(wms)
        print >> self._pFile,'%s'%self._VO       
        print >> self._pFile,'%s'%self._NSAd       
        print >> self._pFile,'%s'%self._LBAd       
        print >> self._pFile,'%s'%self._WMProxy

    def loadBalancingCE(self, vers="LoadBalCE_v2", query=""):
        if self._defCE != None:
            sys.stderr.write("\nWARNING: CE already definied, conflict between load balancing and setCE ?")
            return
        self._defCE = True   
        try:
            lb_class = globals()[vers]
        except:
            sys.stderr.write("ERROR: unknow class "+vers)
            sys.exit(1)
            
        if query != "":
            self._LoadBalCE = lb_class(query)
        else:
            self._LoadBalCE = lb_class()
        
        
    def _FillJDL(self,Appli):       
        if self._delegateProxy == "Yes":
            print >> self._pFile,'%s'%self._MyProxy
        print >> self._pFile,'%s'%self._Rank
        if Appli._MPIcpu != []:
            if self._defCE != None:
               requirmt = 'Requirements = other.GlueCEUniqueID == "%s";' %self._defCE
               ce_out = self._defCE
            else:
                requirmt = 'Requirements = Member("MPI-START", other.GlueHostApplicationSoftwareRunTimeEnvironment)'
                requirmt += '&& Member("OPENMPI", other.GlueHostApplicationSoftwareRunTimeEnvironment)'
                requirmt += '&& ( other.GlueCEStateStatus == "Production" ) '            
                requirmt += '&& ( other.GlueCEInfoTotalCPUs >= %d);'%Appli._MPIcpuTotal            
                ce_out = "unknow"
        else:
            requirmt = self._Req        
            if self._LoadBalCE != None:
                ce = self._LoadBalCE.choiceCE()
                ce_out = ce.split(".")[0] 
                if  ce != None:
                    if G_verbose>=5:  sys.stdout.write("\nSelect CE: %s"%ce)
                    requirmt='Requirements = other.GlueCEUniqueID == "%s";'%ce
            elif self._defCE:
                ce_out = self._defCE.split(".")[0] 
            else:
                ce_out = "unknow"
        print >> self._pFile,'%s'% requirmt
        return ce_out

    def submitCmd(self, Appli):       
        if self._delegateProxy == "Yes":
            if True:
                dpjp = Process('glite-wms-job-delegate-proxy -a -e %s --noint'%self._NameWMS(Appli))
                dpjp.wait(60)
                if not dpjp.isOk():
                    sys.stderr.write("\nERROR:\n commend '%s' NOK\n"%dpjp._cmd)
                    sys.stderr.write(dpjp.stdErr())
                if G_verbose >= 10: sys.stdout.write(dpjp.stdOut())            
        cmd = 'glite-wms-job-submit --noint --config %s -o %s -a %s'%(self._FileConf, Appli._FileID, self._FileJDL)
        return cmd
      
    def statusCmd(self, FileID):        
        cmd = 'unset PYTHONHOME;glite-wms-job-status -i '+FileID + ' --noint --verbosity 1'
        if G_verbose>=10: print cmd
        return cmd
      
    def retrieveCmd(self, FileID, Appli):
        outDir= MyConf().info.workDir+Appli._APCSchedID
        cmd = 'glite-wms-job-output -i '+FileID
        cmd += ' --dir '+outDir+ ' --noint'
        return cmd, outDir

    def _CancelCmd(self, Appli):
        cmd = "glite-wms-job-cancel -i %s --noint"%Appli.getPathFile('ID')
        return cmd

    def _resultCmd(self, fileID):
        cmd='echo a | glite-wms-job-status -i %s --verbosity 2 | awk -F: \'{if(/Current/){s=$NF;n++;} if(/Destination/) {d[$2" "s]++;} } END {for ( x in d ){print  x": "d[x]" / "n;}  }\' | sort'%fileID
        return cmd

#
# Compute Element Load Balancing
#

class LoadBalancingCE:
    def __init__(self):
        self.listCE = []       
        self._update()
        self._lastUpdate = time.time()
        
    def __str__(self): raise
    def _update(self): raise
    def choiceCE(self):
        #  update CE available 
        if ( time.time() - self._lastUpdate)/60 > 10:
            self._lastUpdate =  time.time()
            self._update()
    
    
class LoadBalCE_v1(LoadBalancingCE):
    """Algos CE loadbalancing de Tristan Beau """
    def __init__(self, query=""):
        if query == "":
            self.query = "CEStatus=Production,PlatformArch=x86_64,EstRespTime=0"
        else:
            self.query = query
        LoadBalancingCE.__init__(self)       
        self.ce_idx=0
        self.dec = 1
        
    def __str__(self):
        buffer="\nCEname: FreeSlot"
        for ce in self.listCE:
            #buffer +='\n%s : \t%d'%(ce[0][0:7],ce[1])
            buffer +='\n%s : \t%d'%(ce[0],ce[1])
        return buffer

    def _update(self):
        self.listCE = []
        lce=Process("lcg-info --list-ce --vo %s --attrs WaitingJobs,FreeJobSlots,TotalCPUs --query %s --sed"%(MyConf().gLite.vo, self.query))
        lce.wait(60)
        if not lce.isOk():
            sys.stdout.write(lce.stdOut())
            sys.stderr.write("ERROR ERROR ERROR "+lce.stdErr())
               
        lineout=lce.stdOut().split('\n')        
        for line in lineout:
            fields=line.split('%')
            if len(fields) != 4:
                break
            #t=[fields[0].split(':')[0],int(round(0.9*int(fields[2])))]
            t=[fields[0], int(round(0.9*int(fields[2])))]
            self.listCE.append(t)

        self.listCE.sort(key=operator.itemgetter(1),reverse=True)
        if G_verbose>=5:
            sys.stdout.write("\nCE selection:\n")
            print self

    def choiceCE(self):
        LoadBalancingCE.choiceCE(self)
        if self.listCE[self.ce_idx][1]==0:
            ce=MyConf().gLite.ce
        else:
            ce=self.listCE[self.ce_idx][0]
            self.listCE[self.ce_idx][1] -= self.dec
            if self.listCE[self.ce_idx][1] <= 0:
                if self.ce_idx<len(self.listCE):
                    self.ce_idx +=1
        return ce

 
class LoadBalCE_v2(LoadBalCE_v1):
    """version 1, mais retrie le tableau s'il n'est plus ordonne"""
    def __init__(self,query=""):
        LoadBalCE_v1.__init__(self,query)

    def choiceCE(self):
        LoadBalancingCE.choiceCE(self)        
        if len(self.listCE) == 0:
            return None
        if self.listCE[0][1]==0:
            return None
        else:
            if len(self.listCE) == 1:
                return self.listCE[0][0]
            else:            
                ce=self.listCE[0][0]
                self.listCE[0][1] -= self.dec
                if self.listCE[0][1] < self.listCE[1][1]:
                    self.listCE.sort(key=operator.itemgetter(1),reverse=True)
                return ce

    
class LoadBalCE_v3(LoadBalCE_v1):
    """Repartition au hasard sur les slots libres des CE selectionnes"""
    def __init__(self,query=""):
        LoadBalCE_v1.__init__(self,query)

    def _update(self):
        LoadBalCE_v1._update(self)
        self.ces=[]
        for ce in self.listCE:
            for i in range(ce[1]):
                self.ces.append(ce[0])
        rd.shuffle(self.ces)
        self.nb_ces=len(self.ces)
        self.i_ces=0

    def choiceCE(self):
        LoadBalancingCE.choiceCE(self)
        if self.i_ces >= self.nb_ces:
            ce=MyConf().gLite.ce
        else:
            ce=self.ces[self.i_ces]
            self.i_ces += 1
        return ce

    
       
##################################################
#    
# LOCAL : A faire pour les sub-pipeline
#

class SchedulerLOCAL(SchedulerAbstract):
    def __init__(self):
        SchedulerAbstract.__init__(self)       
        self.Processus = None
        self._retrieveFlag = False
        
    def submit(self, Appli):
        """submit Appli"""
        cmd= 'cd %s; '%(Appli._PathLocWD)
        if  Appli._MPIcpu != []:
            NbNode = Appli._MPIcpu[0]
            NbProc = NbNode*Appli._MPIcpu[1]
            cmd = 'mpirun -np %d %s %s'%(NbProc, Appli._AppliName, Appli._Arg)
        else:
            cmd += '%s %s'%(Appli._AppliName, Appli._Arg)
        if  Appli._keepStdFile:
            self.Processus = Process(cmd, Appli.getPathFile('STDOUT'), Appli.getPathFile('STDERR'))
        else:
            self.Processus = Process(cmd)
        Appli._Status = "SubmitScheduled"
        self.status(Appli)        
        
    def status(self, Appli):
        """Update status attribut of Appli object"""
        if Appli._Status.find('Submit') != 0:
            if G_verbose>10: print "status can't change !!!"
            return
        self.Processus._updateStatus()
        Appli._Status = self.Processus._Status
        Appli._StatusAppli = self.Processus.getExitValue()
        SchedulerAbstract.status(self,Appli)
        
    def retrieveOutput(self, Appli):        
        """Retrieve Appli outputs in work directory"""        
        if not Appli._keepStdFile:
            Appli._stdOut = self.Processus.stdOut()           
            Appli._stdErr = self.Processus.stdErr()
            
        if Appli._stdErr == "":
            os.system('rm '+ Appli.getPathFile("STDERR"))
 
        if not self.Processus.isOk() and G_verbose>10:
            sys.stderr.write("\nERROR with command :\n%s\n"%self.Processus._cmd)
            sys.stderr.write(self.Processus.stdErr())
                    
    def cancel(self, Appli):
        """cancel Application"""
        cmd="kill -2 %d"%self.Processus.pid
        print cmd
        os.system(cmd)

       
###############################################################
#    
# Abstract Class Cluster Scheduler
#

class SchedulerCluster(SchedulerAbstract):
    def __init__(self):
        SchedulerAbstract.__init__(self)
        # Tyep de cluster
        self._TypeCluster = ''
        # name script file
        self._ScriptBatchFile = ''
        # pointor on script file 
        self._ScriptBatchDesc = None

    def _AddSchedulerCommand(self, Appli):
        """Add in batch script scheluder command"""
        pass
    
    def _AddPlateformEnv(self, Appli):
        """Add in batch script specific plateform command"""
        pass
    
    def _AddBeforeRun(self, Appli):
        """Add in batch script user commands before run"""
        pass
    
    def _AddExe(self, Appli):
        """Add in batch script exe"""
        self._ScriptBatchDesc.write("\n\n# Add Application")
        if Appli._MPIcpu == []:
            self._ScriptBatchDesc.write("\n%s %s"%(Appli._AppliName, Appli._Arg))
        else:
            self._AddExeMPI(Appli)
    
    def _AddAfterRun(self, Appli):
        """Add in batch script user commands after run"""
        pass
    
    def _CreateBatchScript(self,  Appli):
        """Create a batch script for cluster scheduler"""        
        # create file
        MyFile = Appli._PathLocWD + Appli._APCSchedID
        self._ScriptBatchFile = MyFile+'.sh'
        self._ScriptBatchDesc = open(self._ScriptBatchFile, "w")
        if  G_verbose > 5: print self._ScriptBatchFile
        # write file 
        
        self._AddSchedulerCommand(Appli)
        self._AddPlateformEnv(Appli)
        self._ScriptBatchDesc.write("\n\ntouch "+self._FileRun)
        self._AddBeforeRun(Appli)
        self._AddExe(Appli)
        self._AddAfterRun(Appli)        
        self._ScriptBatchDesc.write("\n")        
        # close file
        self._ScriptBatchDesc.close()
        os.system('chmod 755 '+self._ScriptBatchFile)

#
# BQS, CCIN2P3 scheduler
#

class SchedulerBQS(SchedulerCluster):
    def __init__(self):
        SchedulerCluster.__init__(self)
        self._TypeCluster = 'BQS'
        self._FileRun = ''        
        self.SecToUISec = 35 # Convention Seconde UI vers seconde walltime


    def _AddAfterRun(self, Appli):
        # save return calu in file
        self._ScriptBatchDesc.write("\necho $? >> "+self._FileRun)
        
    def submit(self, Appli):
        """submit Appli"""
        Appli._CPUTime = _ConvHHMMSSInSec(Appli._CPUTime)
        if Appli._CPUTimePerWeek != '':
            Appli._CPUTimePerWeek = _ConvHHMMSSInSec(Appli._CPUTimePerWeek)
        else:
            Appli._CPUTimePerWeek = 0
        Appli._farm = 'anastasie'
        self._CreateBatchScript(Appli)
        os.putenv('BQSCLUSTER', Appli._farm) 
        myp = Process("qsub "+self._ScriptBatchFile)
        myp.wait(5)
        if myp.isOk():
            Appli._Status = 'SubmitScheduled'
            Appli._TimeStart = time.time()
        else:
            Appli._Status = 'FinishSubmitNOK'
            
    def cancel(self, Appli):
        os.putenv('BQSCLUSTER', Appli._farm)
        mp=Process("qdel "+ Appli._APCSchedID)
        mp.wait()
               
    def status(self, Appli):
        if Appli._Status.find('Submit') != 0:
            if G_verbose>10: print "status can't change !!!"
            return
        if os.path.isfile(self._FileRun):
            Appli._Status = 'SubmitRunning'
        finish = False
        if os.path.isfile(Appli.getPathFile("STDOUT")):
            finish = True
        elif os.path.isfile(Appli.getPathFile("STDERR")):
            finish = True
        if finish:
            rvs = _readFile(self._FileRun)            
            try:
                rv = int(rvs)
                Appli._StatusAppli = rv
                if rv == 0 : Appli._Status = 'FinishOK'
                else: Appli._Status = 'FinishNOK'
            except:
                Appli._Status = 'FinishNOK'            
            os.system('rm -rf '+self._FileRun)
            os.system('rm -rf '+self._ScriptBatchFile)
        if G_verbose>5: print "status :"+Appli._Status
        
        # manage time out and call retrieve output
        SchedulerAbstract.status(self,Appli)
        
    def _AddBeforeRun(self, Appli):
        Appli._AddEnv(self._ScriptBatchDesc)
        if Appli._MPIcpu != []:
            # Use OpenMPI 
            self._ScriptBatchDesc.write("\n. /usr/local/shared/bin/openmpi_env.sh")
        
    def _AddExeMPI(self, Appli):
        NbNode = Appli._MPIcpu[0]
        NbProc = NbNode*Appli._MPIcpu[1]
        if NbProc > 1:
            # Use OpenMPI 
            self._ScriptBatchDesc.write("\n/usr/local/openmpi/bin/mpirun -x LD_LIBRARY_PATH -x PATH --mca pls_rsh_agent /usr/local/products/bqs/bqsrsh -machinefile $BQS_PROCLISTPATH -np $BQS_PROCNUMBER %s %s"%(Appli._AppliName, Appli._Arg))
        else:
            # OpenMPI but without mpirun
            self._ScriptBatchDesc.write("\n%s %s"%(Appli._AppliName, Appli._Arg))

    def _AddSchedulerCommand(self, Appli):
        """Add in batch script scheluder command"""
        # create file       
        self._FileRun = Appli._PathLocWD + Appli._APCSchedID+'.run'
        
        pf = self._ScriptBatchDesc
        pf.write("#!/bin/bash")
        pf.write("\n#PBS -N %s"%Appli._APCSchedID)
        pf.write("\n#PBS -l platform=LINUX")            
        pf.write("\n#PBS -l M=%dMB"%Appli._MemorySize)
        if Appli._CPUTimePerWeek == 0:
            MyTime = Appli._CPUTime*self.SecToUISec
            pf.write("\n#PBS -l T=%d"%MyTime)
            # check class J
            if (Appli._MemorySize > 2200) or (MyTime > 2400000):
                pf.write("\n#PBS -q J")
        else:
            MyTime = Appli._CPUTimePerWeek*self.SecToUISec
            pf.write("\n#PBS -l T=%d"%MyTime)
            pf.write("\n#PBS -q V")
            pf.write("\n#PBS -V")   
        pf.write("\n#PBS -o "+Appli.getPathFile("STDOUT"))
        pf.write("\n#PBS -e "+Appli.getPathFile("STDERR"))
        if Appli._MPIcpu == []:            
            if Appli._AccessDir != '':
                pf.write("\n#PBS -l "+Appli._AccessDir)
        else:           
            NbNode = Appli._MPIcpu[0]
            NbProc = NbNode*Appli._MPIcpu[1]                 
            if NbProc > 1:
                Appli._farm = 'pistoo'
                if Appli._AccessDir != '':
                    pf.write("\n#PBS -l %s=%d"%(Appli._AccessDir, NbProc))
                pf.write("\n#PBS -l ptype=OpenMPI")   
                StringCPU = "\n#PBS -l proc=%d"%(NbProc)       
                if NbNode != NbProc:
                    StringCPU += ",machine=%d"%(NbNode)
                pf.write(StringCPU)
            else:
                # with only one CPU go to anastasie
                if Appli._AccessDir != '':
                    pf.write("\n#PBS -l "+Appli._AccessDir)
                
#
# SGE, Sun Grid Engine
#

class GridEngineCC:
    """
    check des ressources
    """
    pa_mame    = ["pa_short","pa_medium","pa_long"]
    pa_memLim  = [500, 3*1024, 4*1024]
    pa_timeLim = [6*60, 5*3600, 30*3600]
    pa_cpuMax = 112
    huge_time = 46*3600
    huge_mem  = 16*1024

    
    def __init__(self,  memMB, cpuTime, nbCPU=1):
        self.memMB = memMB
        self.cpuTime = cpuTime
        self.nbCPU = nbCPU


    def checkResource(self):
        if self.nbCPU > 1:
            return self._checkResourceParal()
        else:
            return self._checkResourceSeq()

                
    def _checkResourceSeq(self): 
        memMax  = GridEngineCC.huge_mem 
        timeMax = GridEngineCC.huge_time       
        if self.memMB > memMax:
            return "ERROR: too many memory, max is %dMB"%memMax
        if self.cpuTime > timeMax:
            return "ERROR: too many time, max is %f hours"%(timeMax/3600)
        return "Ok"
        
        
    def _checkResourceParal(self):
        memMax  = GridEngineCC.pa_memLim[-1]
        timeMax = GridEngineCC.pa_timeLim[-1]
        cpuMax = GridEngineCC.pa_cpuMax 
        if self.nbCPU > cpuMax:
            return "ERROR: too many CPUs for openmpi GE env., max is %d"%cpuMax
        if self.memMB > memMax:
            return "ERROR: too many memory for MPI job, max is %dMB"%memMax
        if self.cpuTime > timeMax:
            return "ERROR: too many time for MPI job, max is %f hours"%(timeMax/3600)
        return "Ok"
     
    
    def retQueueParal(self):
        nbClass = len(GridEngineCC.pa_mame)
        imem = -1
        for i in range(nbClass):
            if (self.memMB <= GridEngineCC.pa_memLim[i]):
                imem= i
                break        
        itime = -1
        for i in range(nbClass):
            if (self.cpuTime <= GridEngineCC.pa_timeLim[i]):
                itime = i
                break
        if imem>=0 and itime>=0:
            return GridEngineCC.pa_mame[ max(imem, itime) ]
        else:
            return None


class SchedulerSGE(SchedulerBQS):
    def __init__(self):
        SchedulerCluster.__init__(self)
        self._TypeCluster = 'SGE'
        self._FileRun = ''        

    def _AddAfterRun(self, Appli):
        # save return calu in file        
        self._ScriptBatchDesc.write("\nres=$? ")
        self._ScriptBatchDesc.write("\nexec 3<> "+self._FileEnd)
        self._ScriptBatchDesc.write("\necho $res >> "+self._FileEnd)
        self._ScriptBatchDesc.write("\nexec 3>&-")
       
    def submit(self, Appli):
        """submit Appli"""
        Appli._CPUTime = _ConvHHMMSSInSec(Appli._CPUTime)
        if Appli._CPUTimePerWeek != '':
            Appli._CPUTimePerWeek = _ConvHHMMSSInSec(Appli._CPUTimePerWeek)
        else:
            Appli._CPUTimePerWeek = 0       
        self._CreateBatchScript(Appli)       
        myp = Process("qsub "+self._ScriptBatchFile)
        myp.wait(10)
        if myp.isOk():
            Appli._Status = 'SubmitScheduled'
            Appli._TimeStart = time.time()
        else:
            Appli._Status = 'FinishSubmitNOK'
            
    def cancel(self, Appli):       
        mp=Process("qdel "+ Appli._APCSchedID)
        mp.wait()
               
 
    def _AddBeforeRun(self, Appli):
        Appli._AddEnv(self._ScriptBatchDesc)
        if Appli._MPIcpu != []:
            # Use OpenMPI 
            self._ScriptBatchDesc.write("\n. /usr/local/shared/bin/openmpi_env.sh")
        
    def _AddExeMPI(self, Appli):
        NbNode = Appli._MPIcpu[0]
        NbProc = NbNode*Appli._MPIcpu[1]
        if NbProc > 1:
            # Use OpenMPI 
            self._ScriptBatchDesc.write("\nmpiexec --mca btl ^udapl,openib --mca btl_tcp_if_include eth0  -n $NSLOTS %s %s"%(Appli._AppliName, Appli._Arg))
        else:
            # OpenMPI but without mpirun
            self._ScriptBatchDesc.write("\n%s %s"%(Appli._AppliName, Appli._Arg))

    def _AddSchedulerCommand(self, Appli):
        """Add in batch script scheluder command"""
        # create file       
        self._FileRun = Appli._PathLocWD + Appli._APCSchedID+'.run'
        self._FileEnd = Appli._PathLocWD + Appli._APCSchedID+'.end'
        
        pf = self._ScriptBatchDesc
        pf.write("#!/bin/bash -l")
        pf.write("\n#$ -N %s"%Appli._APCSchedID)                
        pf.write("\n#$ -l vmem=%dM"%Appli._MemorySize)
        if Appli._CPUTimePerWeek == 0:
            MyTime = Appli._CPUTime
            pf.write("\n#$ -l ct=%d"%MyTime)
        else:
            MyTime = Appli._CPUTimePerWeek*self.SecToUISec
            pf.write("\n#$ -l ct=%d"%MyTime)
            pf.write("\n#$ -q demon")
            pf.write("\n#$ -V")
            
        # check ressource
        NbProc = 1
        if Appli._MPIcpu != []:            
            NbNode = Appli._MPIcpu[0]
            NbProc = NbNode*Appli._MPIcpu[1]
        sge = GridEngineCC(Appli._MemorySize, MyTime, NbProc)  
        ret = sge.checkResource()
        if ret != "Ok":
            sys.stderr.write(ret)
            raise        
        
        pf.write("\n#$ -o "+Appli.getPathFile("STDOUT"))
        pf.write("\n#$ -e "+Appli.getPathFile("STDERR"))          
        if NbProc > 1:
            queue = sge.retQueueParal()
            if queue == None:
                sys.stderr.write("ERROR: can't define parallele queue for %dMB, %dseconds"%(Appli._MemorySize, MyTime))
            pf.write("#$ -pe openmpi %d -q %s"%(NbProc, queue) )                  
        if Appli._AccessDir != '':
            pf.write("\n#$ -l %s=1"%Appli._AccessDir)         


    def status(self, Appli):
        if Appli._Status.find('Submit') != 0:
            if G_verbose>10: print "Status can't change !!!"
            return
        if os.path.isfile(self._FileRun):
            Appli._Status = 'SubmitRunning'

        # end condition is not robust to user kill or executable error ... to improve
        try:
            finish = os.path.isfile(self._FileEnd)
        except:
            finish = False
        if finish:                
            rvs = _readFile(self._FileEnd)
            print "run:"+rvs
            try:
                rv = int(rvs)
                Appli._StatusAppli = rv
                if rv == 0 : Appli._Status = 'FinishOK'
                else: Appli._Status = 'FinishNOK'
            except:
                print "\nCan't convert:'%s'"%rvs
                Appli._Status = 'FinishNOK'            
            os.system('rm -rf '+self._FileRun)
            os.system('rm -rf '+self._FileEnd)
            os.system('rm -rf '+self._ScriptBatchFile)
        if G_verbose>5: print "Status :"+Appli._Status
        
        # manage time out and call retrieve output
        SchedulerAbstract.status(self,Appli)
                       
                       
###############################################################
#
#  Class Application 
#
###############################################################


class Application:
    def __init__(self, Appli, label=""):
        self._AppliName = Appli
        # if Appli is /bin/echo AppliOnly is echo 
        self._AppliOnly = Appli[Appli.rfind('/')+1:]
        if label=="":			 
            self._APCSchedID = self._AppliOnly
        else:
            self._APCSchedID = label.replace(' ','_')
        self._Arg = ''
        # if [1,1] is not a MPI executable, [4,2] total CPU is 4.2=8 CPUs
        self._MpiNodeCPUbyNode = [1,1]
        # CPU time necessary hh::mm:ss   , memory in MB
        self._StatusAppli = -1 # exit value at end executable
        self._Status = 'NotSubmit' # exit value at end executable
        self._ListSrc = []		
        self._ListInput = []
        self._ListOutput = []
        self._ListInputSE = []
        self._ListOutputSE = []
        self._PathLocWD = ''   # Path		  
        self._FileIDgrid = ''
        self._Timer  = 10
        self._TimeOutToStart = 0
        self._TimeStart = 0
        self._RetrieveWithID = True
        self._RetrieveSEWithID = True
        self._CPUTime = '1:0:0'
        self._CPUTimePerWeek = ''
        self._AccessDir = ''
        self._MemorySize = 1024
        self._LocalDiskSize = 1024
        self._MPIcpu = []
        self._MPIcpuTotal = 0
        self._MainSE = ''
        self._WorkDirSE = '%s/APCScheduler'%os.getenv('USER')
        self._OutDirSE = None
        self._MainScript = None
        self._FlagResubmit = False
        self._keepStdFile= True
        self._stdOut= ''
        self._stdErr= ''
        self._wmsUsed = set([])
        self._wmsIdx = None
        self._threadCopy = None
        
    def _isAppliExist(self, namefile):
        """Return absolute name of namefile and None if doesn't exit"""
        return None
    
    def _Cleaner(self):
        pass
    
    def _FillFileJDL(self):
        pass
    
    def _AddEnv(self, File):
        pass
    
    def copyTarBallOnSE(self, FileSE):
        sys.stderr.write("\nNot Available with kind of application")
        return False
    
    def setArg(self, Arg):
        """Define excutable/script argument.
        Fomrat: string"""
        self._Arg = Arg
       
    
    def setInput(self, List):
        """[Specific grid] Define local input file(s).
        Format : string list ['a','b']"""
        self._ListInput = List
        
    def setInputSE(self, List):
        """[Specific grid] Define input file(s) in storage element.
        Format : string list ['a','b']"""
        self._ListInputSE = List
        
    def setOutput(self, List, AddID=True):
        """[Specific grid] 
        List : Define output file List to retrieve at the end of job via OutputSandbox and copy in job repository
        AddID : add prefixe identificator 
            AddID==True : prefix is SchedulerID random number and letter
            AddID is type String : prefix is value of AddID
            else  : no ID
        """
        self._ListOutput = List 
        self._RetrieveWithID = AddID
                     
                    
    def setOutputSE(self, List, AddID=True):
        """[Specific grid] 
        List  : Define output file List to retrieve at the end of job on storage element
        AddID : add prefixe identificator 
            AddID==True : prefix is SchedulerID random number and letter
            AddID is type String : prefix is value of AddID
            else  : no ID        
        """
        self._ListOutputSE = List
        
        # see _processPrefix 
        self._RetrieveSEWithID = AddID
 
    
    def setOutputDirSE(self, outdir, AddID=False):
        """[Specific grid tarball] Define directory on SE where all files in tarball directory toSE/ will copied, if AddID is True APCScheduler add prefixe ID job to name file"""
        self._OutDirSE = outdir
    
    def setMPI(self, NodeCPUbyNode):
        """Define node number [Node, CPUbyNode]. Example: [4,2] total CPU is 4.2=8"""
        self._MPIcpu = NodeCPUbyNode         
    
    def setCPUTime(self, TimeMem):
        """Define CPU Time request for job.
        Format : string [[hh:]mm:]ss"""
        self._CPUTime = TimeMem
        
    def setCPUTimePerWeek(self, TimeMem):
        """Define CPU Time per week for long and slow job (like class V with BQS).
        Format : string [[hh:]mm:]ss"""
        self._CPUTimePerWeek = TimeMem
    
    def setAccessDirectory(self, Dir):
        """[specific cluster] Requirement name global aria space disk. For example 'sps_planck' in CCIN2P3       
        Fomrat : string """ 
        self._AccessDir = Dir
    
    def setMemorySize(self, Mem):
        """Define memory request for job.
        Format : Integer in MByte"""
        self._MemorySize = Mem
        
    def setLocalDiskSize(self, Mem):
        """[specific cluster] Requirement local worker node space disk in MByte """
        self._LocalDiskSize = Mem
    
    def setStorageElement(self, se):
        """[specific grid] Define current storage element"""
        self._MainSE = se
                 
    def getFile(self,Key, index = 0):
        """ Give name file for follow keywords: 'STDOUT', 'STDERR', 'OUT', 'OUT_SE', 'SUBOUT', 'SUBERR', 'TAR','ID'"""
        if Key=='STDOUT':
            return self.getNameStdOut()
        elif  Key=='STDERR':
            return self.getNameStdErr()
        elif Key=='OUT':
            return self.getNameOutput(index)
        elif Key=='OUT_SE':
            return self.getNameOutputSE(index)
        elif Key=='SUBOUT':
            return self._APCSchedID + '.sub_out'
        elif Key=='SUBERR':
            return self._APCSchedID + '.sub_err'
        elif Key=='TAR':
            return self._APCSchedID + '.tar.gz'
        elif Key=='ID':
            return self.getNameJobID()
        else:
            print "Keyword '%s' is unknown"%Key
            return None
    
    def getPathFile(self, Key, index = 0):
        """Same thing like getFile but with absolute path"""
        name = self.getFile(Key,index)        
        if name == None:
            return None           
        else:
            if G_verbose>=10 : print name
            return os.path.join(self._PathLocWD, name)
            
    def getNameStdOut(self):        
        return (self._APCSchedID +'.stdout')
    
    def getNameStdErr(self):
        return (self._APCSchedID +'.stderr')
         
    def isFinish(self):
        return (self._Status.find('Finish') >= 0)
    
    def isSubmit(self):
        return (self._Status.find('Submit') == 0)
    
    
    def _processPrefix(self, AddID):
        if AddID == True:
            #print self._APCSchedID+"_"  
            return self._APCSchedID+"_"            
        elif isinstance(AddID, str):
            return AddID
        else:
            return ""
        
        
    def getNameOutput(self, idx):
        if idx < len(self._ListOutput):
            filename = self._ListOutput[idx]
            withID = self._processPrefix(self._RetrieveWithID)
        else:
            myidx = idx-len(self._ListOutput)         
            if (myidx < len(self._ListOutputSE)):
                aa = self._ListOutputSE[myidx].split('/')
                filename = aa[len(aa)-1]              
                withID = self._processPrefix(self._RetrieveSEWithID)
            else:
                sys.stderr.write("[getNameOutput]Error out of range")
                sys.exit(1)            
        return (withID + filename)
    
    def getNameJobID(self):
        return (self._APCSchedID +'.id')
    
    def getNameOutputSE(self, idx):
        return self.getNameOutput(idx+len(self._ListOutput))
    
    def isOk(self):
        return (self._Status == 'FinishOK')
    
    def noKeepStdFile(self):
        """ no keep standard output and error file"""
        self._keepStdFile= False



##################################################
#    
# Application type executable/script stand alone
#

class AppliExe(Application):
    def __init__(self, NameExe, label=""):
        AbsNameExe = self._isAppliExist(NameExe)
        if AbsNameExe != None:
            Application.__init__(self, AbsNameExe,label)
        else:
            sys.stderr.write("\nDon't find %s anywhere, check var. env PATH, wms-proxy problem.\n"%NameExe)
            sys.exit(1)
            

    def _isAppliExist(self, NameExe):
        AbsNameExe = _AbsolutePathExe(NameExe)
        if AbsNameExe == None:
            #print "ERROR: Can't find '"+NameExe+"' executable."
            return None

        return AbsNameExe

    def _FillFileJDL(self):
        buffer = 'Executable = "%s";\n'%self._AppliOnly
        buffer += 'Arguments  = "%s";\n'%self._Arg       
        intemp = 'InputSandbox = {"%s"'%self._AppliName
        for elt in self._ListInput:
            intemp += ',"%s"'%elt
           
        for elt in self._ListSrc:
            intemp += ',"%s"'%elt
           
        buffer += intemp +'};\n'
        return buffer
    
    def _FillFileJDLEnv(self):
        buffer = '"LCG_CATALOG_TYPE=lfc","VO_NAME=%s"'%MyConf().gLite.vo
        if os.environ.has_key('LFC_HOST'):            
            buffer += ',"LFC_HOST='+os.environ['LFC_HOST']+'"'
        return buffer

    def _AddEnv(self, File):
        """ env for cluster """
        Val=os.getenv('PATH')
        if Val != None:            
            File.write("\nexport PATH="+Val)
        Val=os.getenv('LD_LIBRARY_PATH')           
        if Val != None: 
            File.write("\nexport LD_LIBRARY_PATH="+Val)

    def _RetrieveFromSE(self):
        # Nothing to do
        pass

#
# python
#

class AppliPython(AppliExe):
    def __init__(self, NameExe,label=""):
        AppliExe.__init__(self, NameExe,label)       

        
    def _AddEnv(self, File):
        """Python env for cluster """
        AppliExe._AddEnv(self, File)
        Val=os.getenv('PYTHONHOME')
        if Val != None:            
            File.write("\nexport PYTHONHOME="+Val)
        Val=os.getenv('PYTHONPATH')           
        if Val != None: 
            File.write("\nexport PYTHONPATH="+Val)

#
# parachute 
#


# Design pattern STATE to manage re-usable tarball on SE
# ... finally only 2 cases, I keep design pattern state approach but not necessary
#
class _AppliParachute_TarExist:
    def doTarBallIfNecessary(self, master):
        """ call by submit """
        # nothing to do, tarball exist on SE
        return self
    
    def doTarBallAndCopy(self, master, pathSE):
        """ call by job.copyTarBallOnSE() """
        # nothing to do tar exist already
        sys.stdout.write("\nWARNING: tarball '%s' already exist !"%master._nameTarball)
        return self
    

class _AppliParachute_DoTar:
    def doTarBallIfNecessary(self, master):
        """ call by submit """
        # Tar doesn't exist and user doesn't ask to save it
        master._DoTarBall()       
        return _AppliParachute_State._tarExist
        
    def doTarBallAndCopy(self, master, pathSE):
        """ call by job.copyTarBallOnSE() """
        # Tar doesn't exist and user ask to save it
        master._DoTarBall(pathSE)       
        return _AppliParachute_State._tarExist
    
   
# container class   
class _AppliParachute_State:
    _tarExist = _AppliParachute_TarExist()
    _doTar   = _AppliParachute_DoTar()
    
        
class AppliParachute(AppliExe):
    S_listTarballExist= []
    def __init__(self, NameExe, MainScript = None, label="", KeepTarBall=False):
        """MainScript Optional, define the main script of tarball,
        ie APCScheduler will called this script in tarball case. If not
        present the executable will called directly"""
        
        AppliExe.__init__(self, NameExe, label)
       
        self._KeepTarBall = KeepTarBall
                
        if MainScript != None:
            AbsNameExe = _AbsolutePathExe(MainScript)
            if AbsNameExe == None:
                sys.stderr.write("ERROR: Can't find '"+MainScript+"' executable.\n" )
                sys.exit(1)
            else:
                self._MainScript = AbsNameExe
        
    def _isAppliExist(self, NameExe):        
        AbsNameExe = AppliExe._isAppliExist(self, NameExe)
        if AbsNameExe == None:
            # may be on SE ?
            TarOk = False
            se=SEtools( MyConf().gLite.vo,MyConf().gLite.ce )
            if NameExe in AppliParachute.S_listTarballExist:
                TarOk = True
            else:                
                if se.sizeFile(NameExe) > 0:
                    TarOk = True
                    AppliParachute.S_listTarballExist.append(NameExe)
                    if G_verbose>=5:
                        print 'AppliParachute.S_listTarballExist:', AppliParachute.S_listTarballExist
            if  TarOk:   
                # NameExe exist on SE
                AbsNameExe = NameExe
                self._nameTarball = se._simpleDelTag(NameExe)
                self._stateObj = _AppliParachute_State._tarExist
            else:                
                return None
        else:
            # file tar to do
            self._stateObj = _AppliParachute_State._doTar 
        return AbsNameExe

    def _FillFileJDL(self):
        # do ScriptBoot.sh
        BootFile = os.path.join(self._PathLocWD,"BootScript.sh") 
        self._DoScriptBoot(BootFile)
        
        # create temp directory on SE
        se=SEtools( MyConf().gLite.vo, self._MainSE)
        pathdir = '%s/%s'%(self._WorkDirSE,self._APCSchedID)
        se._mkdirSE(pathdir)

        # do tarball ?
        self._stateObj.doTarBallIfNecessary(self)
        
        bufferJDL = 'Executable = "BootScript.sh";\n'
        if self._MPIcpu !=[]:    
            NbNode = self._MPIcpu[0]
            self._MPIcpuTotal = NbNode*self._MPIcpu[1]            
            #bufferJDL += 'JobType  = "Normal";\n'
            bufferJDL += 'JobType  = "MPICH";\n'
            bufferJDL += 'CPUNumber = %d;\n'%self._MPIcpuTotal
            
        if self._WorkDirSE == None:
            sys.stderr.write("ERROR: You must define SetWorkDirSE()")
            raise
        
        # argument bootscript is the tarball path on SE
        # chemin du tar sur le SE, workdir  SE, argument du job
        strArg = "%s "%self._nameTarball 
        strArg+= "%s/%s "%(self._WorkDirSE,self._APCSchedID)
        strArg+= "%s"%self._Arg        
            
        bufferJDL += 'Arguments  = "%s";\n'%strArg
        bufferJDL += 'Environment = {'+self._FillFileJDLEnv()+'};\n'
        intemp = 'InputSandbox = {"%s"'%BootFile
        for elt in self._ListInput:
            intemp += ',"%s"'%elt
            #print elt
 
        bufferJDL += intemp+'};\n'
        return bufferJDL
    
    def _RetrieveFromSE(self):
        se=SEtools( MyConf().gLite.vo, self._MainSE)        
        # copy dir UI
        pathdir = '%s/%s'%(self._WorkDirSE,self._APCSchedID)       
        self._threadCopy = _CopySEThread(se, pathdir, self._PathLocWD, self._APCSchedID+'_')
        self._threadCopy.start()

        
    def _Cleaner(self):
        if self._threadCopy == None:
            # pas de rapatirement de fichier en cours pour ce job
            # on oeut tout effacer
            pathdir = '%s/%s'%(self._WorkDirSE,self._APCSchedID)
            cmd='apcgrid-rm -r '+pathdir
            mp = Process(cmd)
            mp.wait()
        else:
            # on attend la fin du thread qui detruira la repertoire temporaire
            if G_exit : self._threadCopy.join()

        
    def _DoTarBall(self, pathTar=""):
        if self._FlagResubmit: return        
        if G_verbose>=10: sys.stdout.write("\ndef _DoTarBall(self, pathTar=""):\n")
        root = os.path.join(self._PathLocWD, self._APCSchedID)
        TarRoot = os.path.join(root, "tarball")       
        os.makedirs(TarRoot)
        os.makedirs(TarRoot+"/lib")
        os.makedirs(TarRoot+"/toSE")                
        os.makedirs(TarRoot+"/toUI")                
        os.makedirs(TarRoot+"/python")

        # copy exe and script
        os.system("cp %s %s "%(self._AppliName, TarRoot))
        namescript = TarRoot+"/tarballScript.py"
        self._DoScriptTarball(namescript)       
        if self._MainScript != None: os.system("cp %s %s"%(self._MainScript, TarRoot))
        
        # copy input list 
        for elt in self._ListInput: os.system("cp -r %s %s"%(elt, TarRoot))          

        # add lib
        filesrc = __file__
        if filesrc[-1]=='c':
            filesrc = filesrc[0:-1]
            if G_verbose>10: sys.stdout.write("add %s, not %s\n" %(filesrc,  __file__))
        os.system("cp -r %s %s/python"%(filesrc, TarRoot))
        listlib = []
        if not _FindLib(self._AppliName, listlib):
            sys.exit(1)

        for elt in listlib:
            cmd ="cp %s %s/lib"%(elt, TarRoot)
            os.system(cmd)
            if G_verbose>=10: print cmd
        
        # tar
        cmd = "cd %s; tar cfz %s *"%(TarRoot, self.getPathFile('TAR'))
        #submitCmd(cmd,-1,False) pourquoi ça bloque ?
        os.system(cmd)
        os.system("rm -rf "+root)

        # copy to SE        
        se=SEtools( MyConf().gLite.vo, self._MainSE)
        if  pathTar== "":
            # cas ou le tarball est SE temporaire, 
            self._nameTarball='%s/%s/%s'%(self._WorkDirSE,self._APCSchedID,self.getFile("TAR"))
        else:
            # cas ou le tarball sera reutilise
            if se.sizeFile(pathTar) >=0:
                sys.stderr.write("\nFile '%s' exist !! Delete it or change name\n"%pathTar)
                sys.exit(1)
            self._nameTarball=se._simpleDelTag(pathTar)
        ret=se.cp(self.getPathFile('TAR'), 'se:'+self._nameTarball)
        if ret != 0:
            if G_verbose>1: sys.stderr.write("ERROR: Can't copy tarball %s to se:%s\n"%(self.getPathFile('TAR'),self._nameTarball))
            self._Status = 'FinishSubmitNOK'
            return                              
        if not self._KeepTarBall: os.system("rm -rf "+self.getPathFile('TAR'))


    def _DoScriptTarball(self, namefile):
        # python script call by boot script on compute element
        buffer = "import sys\nimport os"
        buffer += "\nsys.path.append('python')" 
        buffer += "\nfrom APCScheduler import *"
        buffer += "\nsetVerboseLevel(1)"
        buffer += "\n\n#update LD_LIBRARY_PATH\naddLIBRARYPATH(os.environ['PWD']+'/lib')"
        buffer += "\nse=SEtools( '%s', '%s') "%(MyConf().gLite.vo, self._MainSE)
        buffer += "\n\n# copy input SE"       
        for elt in self._ListInputSE:
            buffer += "\nse.cp('se:%s','%s')"%(elt,elt.split('/')[-1])
        buffer += "\n\n# call executable"
        buffer += "\nArg = ''\nfor i in range(2,len(sys.argv)): Arg += sys.argv[i]+ ' '"
        if self._MainScript == None:
            buffer += "\nret=submitCmd('./%s %%s'%%Arg,-1)"%(self._AppliOnly)
        else:
            buffer += "\nret=submitCmd('./%s %%s'%%Arg,-1)"%(os.path.basename(self._MainScript))
        if self._OutDirSE != None:
            buffer += "\n\n# copy out SE"
            buffer += "\nse.cpdir( 'toSE', 'se:%s') " %self._OutDirSE  
        buffer += "\n\n# copy out UI"
        buffer += "\nse.cpdir( 'toUI', 'se:%s/data'%sys.argv[1])\n"
        buffer += "\nsys.exit(ret)"
        pf = open(namefile,'w+')
        pf.write(buffer)
        pf.close()
        os.system('chmod 755 '+namefile)


    def _DoScriptBoot(self, scriptname):
        # script call by inputsandBox
        if not os.path.isfile(scriptname):
            buffer = "#!/bin/bash"
            buffer += "\nlcg-cp -v --vo $VO_NAME lfn:/grid/$VO_NAME/$1 file:`pwd`/tarball.tar.gz"
            buffer += "\ntar xzf tarball.tar.gz ; rm -rf tarball.tar.gz"
            buffer += "\nexport PATH=$PATH:./"
            buffer += "\nshift"
            buffer += '\n\necho "**************** date debut job code : " `date`'
            buffer += "\npython tarballScript.py $* 2>&1"
            buffer += "\nret=$?"
            buffer += '\necho "**************** date fin job code  : " `date`'
            #buffer += "\n\ncd ..;rm -rf tarball"
            buffer += "\nexit $ret"
            pf = open(scriptname,'w+')
            pf.write(buffer)
            pf.close()
            os.system('chmod 755 '+scriptname)

    def copyTarBallOnSE(self, pathSE):
        """Copy tarball on SE"""
        if self._Status != 'NotSubmit':
            sys.stdout.write("\nWARNING: call copyTarBallOnSE() before submit().\n")
            return
        self._stateObj = self._stateObj.doTarBallAndCopy(self, pathSE)


        
###############################################################
#
#  Class JobClass : schedule one application
#
###############################################################

class JobClass:
    # liste tous les jobs en cours pour suppression (cancel) utilisateur CRTL+C
    dictJobSubmit = {}
    
    def __init__(self, MyAppli, MySched):
        self._Appli = MyAppli
        self._Scheduler = MySched         
        self._Appli._PathLocWD = MyConf().info.workDir
        
##         # id 
##         unique = False
##         Cpt_unique = 0        
##         while not unique:
##             if Cpt_unique >= 0:
##                 self._Appli._APCSchedID = _AddAlea(self._Appli._APCSchedID, 4)            
##             cmd = "[[ `ls %s%s.* 2>/dev/null` == '' ]]"%(self._Appli._PathLocWD, self._Appli._APCSchedID)
##             if os.system(cmd) == 0:
##                 unique = True
##             else:
##                 if Cpt_unique > 10:
##                     sys.stderr.write("\nERROR: Can't unique ID !!!\n")
##                     raise
##                 Cpt_unique += 1
        self._Appli._APCSchedID = _AddAlea(self._Appli._APCSchedID, 6)  
        self._Appli._FileID = self._Appli.getPathFile('ID')
        self._TimeOutToStart = "24:0:0"
        if self._Appli._MainSE == '':
            self._Appli._MainSE = MyConf().gLite.se
            
    def timerUpdateStatus(self, timeUpdate):
        """Time between two update status ,
        timeUpdate[[hh:]mm:]ss string"""
        sec = _ConvHHMMSSInSec(timeUpdate)
        if sec > 0 :
            self._Appli._Timer = sec

    def addFullPath(self, File):
        return (self._Appli._PathLocWD+File)
    
    def copyTarBallOnSE(self, FileSE):
        """Create a tarball and copy it on SE. Call it before submit()"""
        return (self._Appli.copyTarBallOnSE(FileSE))

    def name(self):
        return (self._Appli._APCSchedID)
    
    def status(self):       
        self._Scheduler.status(self._Appli)
        return self._Appli._Status
    
    def isFinish(self):        
        return self._Appli.isFinish()
    
    def isSubmit(self):        
        return self._Appli.isSubmit()
    
    def isOk(self):
        return self._Appli.isOk()
     
    def submit(self,TimeOutToStart="24:0:0"):        
        if G_verbose>15: print 'submit'
        self._Appli._TimeOutToStart = _ConvHHMMSSInSec(TimeOutToStart)
        if G_verbose > 10:print "TimeOutToStart=", self._Appli._TimeOutToStart
        if (self._Appli._Status == 'NotSubmit'):
            JobClass.dictJobSubmit[self._Appli._APCSchedID] = self
            self._Scheduler.submit(self._Appli)
            # add job to submit job
        else:
            print 'Already submit !'

    def wait(self):
        self._Scheduler.wait(self._Appli)
    
    def submitAndWait(self,TimeOutToStart="24:0:0"):
        self.submit(TimeOutToStart)
        self.wait()

    def fullNameStdOut(self):
        return self.addFullPath(self._Appli.getNameStdOut())
    
    def fullNameStdErr(self):
        return self.addFullPath(self._Appli.getNameStdErr())
    
    def fullNameOutput(self, Idx):
        if Idx >= len(self._Appli._ListOutput):
            print "Idx too great"
            return None
        else:
            return self.addFullPath(self._Appli.getNameOutput(Idx))
        
    def stdOut(self):
        return self._Scheduler.stdOut(self._Appli)

    def stdErr(self):
        return self._Scheduler.stdErr(self._Appli)
    
        #return _readFile(self.fullNameStdErr())
    
    def output(self, Idx):        
        return _readFile(self.fullNameOutput(Idx))

    def sendMailResult(self):
        if MyConf().info.mail == '':
            print 'Define address mail with setMail() function'
            return
        cmd = ('mail -s "APCScheduler : %s status is %s" %s')%(self._Appli._APCSchedID,self._Appli._Status,MyConf().info.mail)
        #print cmd
        if self._Appli._Status == 'FinishOK':
            file = self.fullNameStdOut()
            if os.path.isfile(file):
                os.system(cmd + '< %s'%(file))
            else:
                os.system('echo "No file stdout available"|'+cmd )           
        elif self._Appli._Status == 'FinishSubmitNOK':
            file = self._Appli.getPathFile('SUBERR')
            if os.path.isfile(file):
                os.system(cmd + '< %s'%(file))
            else:
                os.system('echo "No file submit stderr available"|'+cmd )
        elif self._Appli._Status == 'FinishTimeOut':
            os.system('echo " "|'+cmd )
        else:
            file = self.fullNameStdErr()
            if os.path.isfile(file):
                os.system(cmd + '< %s'%(file))
            else:
                os.system('echo "No file stderr available"|'+cmd )



###############################################################
#
#  Class MultiJobsClass : schedule N applications
#
###############################################################

class MultiJobsClass:
    S_EventFinish = threading.Event()
    
    def __init__(self, name=""):        
        self._ListJob = []       
        self._Timer = 60*5
        self._name = name
        self._timeStart = time.asctime()
        
    def append(self, job):
        """Add a job to the Run"""
        self._ListJob.append(job)
        
        
    def timerUpdateStatus(self, timeUpdate):
        """Time between two update status ,
        timeUpdate[[hh:]mm:]ss string"""
        sec = _ConvHHMMSSInSec(timeUpdate)
        if sec > 0 :
            self._Timer = sec


    def submitAndWaitAll(self,TimeOutToStart="24:0:0", MaxRunning=0):
        """submit all jobs and wait all:
        TimeOutToStart: [[hh:]mm:]ss string, if job doesn't run after TimeOutToStart abort it.
                        default value is 15 mn ie 15:0
        """
        if MaxRunning <=0:
            self.submitAll(TimeOutToStart)
            self.waitAll()
            return
        
        NbRun = 1
        while NbRun != 0:
            NbRun = 0
            for job in self._ListJob:
                job.status()
                #print "job",job._Appli._APCSchedID, " status ", job._Appli._Status
                if job.isSubmit() :                    
                    NbRun += 1
                
            # submit if slot available            
            if NbRun < MaxRunning:
                for job in self._ListJob:
                    if job._Appli._Status == 'NotSubmit':
                        job.submit(TimeOutToStart)
                        #print "submit ", job._Appli._APCSchedID
                        NbRun += 1
                        if NbRun >= MaxRunning: 
                            break
                        
            # Sleep if not finished
            if NbRun > 0:
                MultiJobsClass.S_EventFinish.wait(self._Timer)
                MultiJobsClass.S_EventFinish.clear()
        

    def submitAll(self,TimeOutToStart="24:0:0"):
        """submit all:
        TimeOutToStart: [[hh:]mm:]ss string, if job doesn't run after TimeOutToStart abort it.
                        default value is 15 mn ie 15:0
        """
        for job in self._ListJob:
            job.submit(TimeOutToStart)            
                    

    def waitAll(self):
        """wait all job in the run"""
        NbRun = 1
        while NbRun != 0:
            NbRun = 0
            for job in self._ListJob:
                if not job.isFinish():
                    job.status()
                    if not job.isFinish() :
                        NbRun += 1
            # Sleep if not finished
            if NbRun > 0:
                MultiJobsClass.S_EventFinish.wait(self._Timer)
                MultiJobsClass.S_EventFinish.clear()


    def concatOutput(self, IdxOutput, FileConcat):
        """Concate output[IdxOutput] file of the list defined by setOutput() application method
           in one file FileConcat
        """
        if  os.path.isfile(FileConcat):
            os.system('rm -rf %s'%FileConcat)
        os.system('touch %s '%FileConcat)
        for job in self._ListJob:
            if job.isOk():
                os.system('cat %s >> %s'%(job.fullNameOutput(IdxOutput),FileConcat))
                if G_verbose>15: print 'add file '+ job.fullNameOutput(IdxOutput)
            

    def concatEachOutput(self, PrefixeFileConcat):
        """Concatenation of all output file """
        for i in range(len(self._ListJob[0]._Appli._ListOutput)):
            self.concatOutput(i, PrefixeFileConcat+'_'+self._ListJob[0]._Appli._ListOutput[i])
 

    def concatStdOut(self, FileConcat):
        """Concatenation of all sdtout"""
        if  os.path.isfile(FileConcat):
            os.system('rm -rf %s'%FileConcat)
        os.system('touch %s '%FileConcat)
        for job in self._ListJob:
            if job.isOk():
                os.system('cat %s >> %s'%(job.fullNameStdOut(),FileConcat))
                if G_verbose>15: print 'add file '+ job.fullNameStdOut()


    def concatStdErr(self, FileConcat):
        """Concatenation of all sdterr"""
        if  os.path.isfile(FileConcat):
            os.system('rm -rf %s'%FileConcat)
        os.system('touch %s '%FileConcat)
        for job in self._ListJob:            
            os.system('cat %s >> %s'%(job.fullNameStdErr(),FileConcat))
            if G_verbose>15: print 'add file '+ job.fullNameStdErr()


    def concatId(self, FileConcat=""):
        """Concatenation of all sdterr"""
        if  os.path.isfile(FileConcat):
            os.system('rm -rf %s'%FileConcat)
        os.system('touch %s '%FileConcat)
        for job in self._ListJob:            
            os.system('cat %s >> %s'%(job._Appli.getPathFile('ID'),FileConcat))
            

    def result(self):       
        NbOk = 0
        NbNOk = 0
        NbTimeOut = 0
        NbTot= len( self._ListJob)
        for job in self._ListJob:           
            if job.isOk():
                NbOk += 1
            elif job.status().find("FinishNOK")>=0:
                NbNOk += 1
            elif job.status() == "FinishTimeOut":
                NbTimeOut += 1

        buffer = "output directory : %s\nResult:\n"%job._Appli._PathLocWD
        buffer += "  %d job(s) OK on %d\n"%(NbOk, NbTot)
        if NbNOk > 0:
            buffer += "  %d job(s) NOK\n"%(NbNOk)
        if NbTimeOut > 0:
            buffer += "  %d job(s) time out to pass RUNNING\n"%(NbTimeOut)
       
        NbOtherNOK = NbTot-NbOk-NbNOk-NbTimeOut
        if NbOtherNOK > 0:
            buffer += "  %d job(s) failed to other reason\n"%NbOtherNOK      
        buffer += self._ListJob[0]._Scheduler._resultListJob(self)
        if NbTot != NbOk :
            buffer += "\njob(s) failed information:\n--------------------------\n"
        for job in self._ListJob:           
            if not job.isOk():
                buffer += "status job %s is %s"%(job.name(), job.status())
                if job.status() == 'FinishNOK':
                    buffer +=" with status %d."%job._Appli._StatusAppli
                buffer +="\n"
                err = job.stdErr()
                if err != None and err != '':
                    buffer += "stderr:\n"+err+"\n"                   
                    buffer += "===============================================================================\n\n"
        return buffer

       
    def printResult(self):
        print self.result()


    def sendMailResult(self):
        if MyConf().info.mail == '':
            sys.stderr.write('\nERROR sendMailResult() method:\nno address mail, set it with setMail() function')
            return
        FileResult = 'tmpresult.txt'
        pf = open(FileResult,'w+')
        pf.write(self.result())
        pf.close()       
        cmd = 'mail -s "APCScheduler: result run %s started %s" %s < %s'%(self._name, self._timeStart, MyConf().info.mail, FileResult)
        os.system(cmd)
        time.sleep(1)
        os.system('rm -rf '+FileResult)
