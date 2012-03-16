from __future__ import absolute_import

import os,sys
import pyrant
import uuid

import acclogger

# minifs.py
# an exceedingly basic mfs-ish filesystem-ish api

class Partition(object):
    """region on disk to store files"""
    
    def __init__(self, path=u'accellion_filestore', capacity=2000):

        self.logger = acclogger.AccellionLogger(namespace=__name__)
        self.logger.setupLogger()
        """logger setup"""

        self.partdir = path
        """partition directory"""
        
        self.partsize = capacity * 1048576
        """accepts megabytes, converts to bytes"""

        self.partremsize = capacity * 1048576
        """remaining partition size, in bytes"""

        self.recordregistry = []
        """registry of all files in record + expiry pos"""
      
        self.sizeregistry = {}

        self.filelocks = set()
        """records names of files that must be locked"""

        if os.path.exists(self.partdir) == False:
            homedir = os.mkdir(self.partdir)
            """creates storage directory"""

    def available(self):
        """returns space remaining in storage conatiner"""
        return self.partremsize

    def capacity(self):
        """returns storage capacity of container"""
        return self.partsize

    def list(self):
        """returns a list of records in filesystem"""
        return self.recordregistry

    def search(self, identifier):
        """returns index if record is in file system"""

        if identifier.encode('utf-8') in self.recordregistry:
            recindx = self.recordregistry.index(identifier.encode('utf-8'))
            if identifier in os.listdir(self.partdir):               
                self.logger.debug('Found record ' + repr(identifier) + ' at ' + str(recindx) + ' in ' + self.__class__.__name__ + '.')
                return recindx
            self.recordregistry.pop(recindx)
        return -1
            
    def get(self, identifier):
        """returns python object file given file identifier"""
        file_index = self.search(identifier)
        if file_index > -1:
            path = self.generate_file_handle(identifier)
            try:
                fp = open(path, 'rb')
            except IOError, e:
                for elem in e:
                    self.logger.error(elem)
                return -1
            return fp
        print 'File not found in ' + self.__class__.__name__
        return -1

    def add(self, identifier, fp):
        """perform storage of file to disk"""
        file_size = 0

        file_size = os.fstat(fp.fileno())[6]

        if file_size >= self.partsize:
            self.logger.debug('File ' + repr(identifier) + ' too large to store. Capacity: ' + str(self.partsize) + 'bytes')
            return -1
        if file_size >= self.partremsize:
            self.logger.debug('Not enough space to store file ' + repr(identifier) + '.')
            return -1
        if self.search(identifier) > -1:
            self.remove(identifier)
        
        self.sizeregistry[identifier] = file_size
        self.partremsize = self.partremsize - file_size
        self.filelocks.add(identifier)
        self.write_disk(identifier, fp)
        self.filelocks.remove(identifier)
        self.recordregistry.insert(0, identifier.encode('utf-8'))
        self.logger.info('inserting ' + repr(identifier))
        self.logger.info('File ' + repr(identifier) + ' successfully stored.')
        return fp
  
    def remove(self, identifier):
        """perform removal of file from disk"""

        if self.search(identifier) is not -1:
            if identifier not in self.filelocks:
                path = self.generate_file_handle(identifier)
                try:
                    self.partremsize = self.partremsize + self.sizeregistry[identifier]
                except KeyError:
                    self.logger.error('Corrupted partition size record.')

                try:
                    self.recordregistry.remove(identifier.encode('utf-8'))
                except ValueError:
                    self.logger.error('Corrupted partition file record.')

                try:
                    del self.sizeregistry[identifier]
                except KeyError:
                    print 'If the size record dict lookup failed here, it should have failed before when calculating remaining partition size.'  

                try:
                    os.remove(path)
                except OSError:
                    self.logger.error("Serious problem, path to this cached file doesn't exist.")
                self.logger.info('File ' + repr(identifier) + ' successfully removed.')
                return 0
            self.logger.error('File in use, could not be removed.')
            return -1
        self.logger.error('Could not find file ' + repr(identifier) + '.')
        return  -1
    
    def update(self, identifier, fp):
        """replace file data of stored file with new file data"""
        if self.search(identifier) is not -1:    
            self.remove(identifier)
            self.add(identifier, fp)
            print 'File ' + repr(identifier) + ' successfully updated.'
            return 0
        print 'Could not find file ' + repr(identifier) + '.'
        return -1

    def generate_file_handle(self, identifier):
        """generates handle for file"""
        if self.partdir.endswith('/'):
            path = self.partdir + identifier
        else:
            path = self.partdir + '/' + identifier
        return path

    def write_disk(self, identifier, fp):
        """writes a file to disk, returns 1 if everything went good"""
        
        path = self.generate_file_handle(identifier)
        try:
            print path.encode('utf-8')
            fp_to_disk = open(path.encode('utf-8'), 'wb')
        except IOError:
            print "Could not open path for file " + repr(identifier) + '.'
            return -1
        tempstr = ""
        
        #file object iteration
        if type(fp) == "<type 'file'>":
            fp.seek(0)
            tempstr = fp.read(4096)
            while tempstr != "":
                fp_to_disk.write(tempstr)
                tempstr = fp.read(4096)
            fp_to_disk.close()
            fp.seek(0)
            return 0
        #generator iteration
        else:
            for tempstr in fp:
                fp_to_disk.write(tempstr)
            fp_to_disk.close()
            return 0

    def read_disk(self, identifier):
        """returns data of a file, if in record. otherwise returns -1"""
        if self.partdir.endswith('/'):
            path = self.partdir + identifier
        else:
            path = self.partdir + '/' + identifier
        try:    
            fp = open(path, 'rb')
            data = fp
            fp.close()
            return data
        except IOError:
            self.logger.error('Could not read file from disk.')
            return -1

    def empty_storage(self):
        while len(self.recordregistry) > 0:
            self.remove(self.recordregistry[0])

class MRUPreserve(Partition):
    """Nature preserve for unique, free-roaming most-recently-used files"""

    def __init__(self, path=u'/home/admin/accellion_filestore', capacity=2000):
        super(MRUPreserve, self).__init__(path, capacity)

    def assess_removals(self, identifier, file_size):
        """determines if we need to make space for new cache items"""
        #check size of file
        #if incoming size is greater than available space
        deleted_list = []
        if file_size >= self.partremsize:
        #start kickin out old data
            while file_size >= self.partremsize:
                if(len(self.recordregistry) == 0):
                    self.partremsize = self.partsize
                    return []
                currentid = self.recordregistry[ -1 ]
                self.remove(currentid)
                deleted_list.append(currentid)
        return deleted_list
            
    def add(self, identifier, fp):
        """perform storage of file to disk"""
        #performs same function as add in Partition class, but
        #    actively removes files if filesize > space remaining        
        deleted = [] #list of removed files, if removed
        file_size = os.fstat(fp.fileno())[6]
        self.sizeregistry[identifier] = file_size
        if file_size >= self.partsize:
            self.logger.debug('File ' + repr(identifier) + ' too large to store. Capacity: ' + str(self.partsize) + 'bytes')
            return -1
        if file_size >= self.partremsize:
            deleted = self.assess_removals(identifier, file_size)
        if self.search(identifier) > -1:
            self.update(identifier, fp)
            return deleted
        self.partremsize = self.partremsize - file_size
        self.write_disk(identifier, fp)
        self.pop_and_insert(identifier)
        self.logger.debug('inserting ' + repr(identifier))
        self.logger.info('File ' + repr(identifier) + ' successfully stored.')
        return deleted

    def update(self, identifier, fp):
        """replace file data of stored file with new file data"""
        #unique to this method, returns a tuple of 0 return value and deleted objects
        if self.search(identifier) is not -1:    
            self.remove(identifier)
            deleted = self.add(identifier, fp)
            self.logger.debug('File ' + repr(identifier) + ' successfully updated.')
            return deleted
        self.logger.error('Could not find file ' + repr(identifier) + '.')
        return -1

    def get(self, identifier):
        """returns python object file given file identifier"""
        file_index = self.search(identifier)
        if file_index > -1:
            path = self.generate_file_handle(identifier)
            try:
                fp = open(path, 'rb')
            except IOError:
                self.logger.info('Could not get() associated file ' + identifier + ' from disk.')
                return -1
            self.pop_and_insert(identifier)
            return fp
        self.logger.debug('File not found in ' + self.__class__.__name__)
        return -1
    
    def pop_and_insert(self, identifier):
        """helper method to reposition an element in a list"""
        try:
            self.recordregistry.pop(self.recordregistry.index(identifier.encode('utf-8')))
        except ValueError:
            #That just means item hasn't been cached recently.  Continue insert!
            pass
        self.recordregistry.insert(0, identifier.encode('utf-8'))

class MRUGobPreserve(MRUPreserve):
    """Exact same thing as MRUPreserve, but overwritten add() method allows use of iterables"""
    def __init__(self, path=u'/home/admin/accellion_filestore', capacity=2000):
        super(MRUGobPreserve, self).__init__(path, capacity)

    def add(self, identifier, fp, size):
        """perform storage of file to disk"""
        #performs same function as add in Partition class, but
        #    actively removes files if filesize > space remaining        
        deleted = [] #list of removed files, if removed
        file_size = size
        self.sizeregistry[identifier] = file_size
        if file_size >= self.partsize:
            self.logger.debug('File ' + repr(identifier) + ' too large to store. Capacity: ' + str(self.partsize) + 'bytes')
            return -1
        if file_size >= self.partremsize:
            deleted = self.assess_removals(identifier, file_size)
        if self.search(identifier) > -1:
            self.update(identifier, fp)
            return deleted
        self.partremsize = self.partremsize - file_size
        self.write_disk(identifier, fp)
        self.pop_and_insert(identifier)
        self.logger.debug('inserting ' + repr(identifier))
        self.logger.info('File ' + repr(identifier) + ' successfully stored.')
        return deleted

