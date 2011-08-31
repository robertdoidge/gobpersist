from __future__ import absolute_import

import os,sys
import pyrant
import uuid

# minifs.py
# an exceedingly basic mfs-ish filesystem-ish api

class Partition(object):
    """region on disk to store files"""
    
    def __init__(self, path='/home/admin/accellion_filestore', capacity=2000):

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
        if identifier in self.recordregistry:
            recindx = self.recordregistry.index(identifier)
            print 'Found record ' + identifier + ' at ' + str(recindx) + '.'
            return recindx
        return -1
            
    def get(self, identifier):
        """returns python object file given file identifier"""
        if self.search(identifier) > -1:
            path = self.generate_file_handle(identifier)
            fp = open(path, 'rb')
            return fp
        print 'File not found.'
        return -1

    def add(self, identifier, fp):
        """perform storage of file to disk"""
        file_size = os.fstat(fp.fileno())[6]
        if file_size >= self.partsize:
            print 'File ' + identifier + ' too large to store. Capacity: ' + str(self.partsize) + 'bytes'
            return -1
        if file_size >= self.partremsize:
            print 'Not enough space to store file ' + identifier + '.'
            return -1
        if self.search(identifier) > -1:
            self.update(identifer, fp)
            return 0
        self.sizeregistry[identifier] = file_size
        self.partremsize = self.partremsize - file_size
        self.filelocks.add(identifier)
        self.write_disk(identifier, fp)
        self.filelocks.remove(identifier)
        self.recordregistry.insert(0, identifier)
        print 'inserting ' + identifier
        print 'File ' + identifier + ' successfully stored.'
        return 0
  
    def remove(self, identifier):
        """perform removal of file from disk"""

        if self.search(identifier) is not -1:
            if identifier not in self.filelocks:
                path = self.generate_file_handle(identifier)
                self.partremsize = self.partremsize + self.sizeregistry[identifier]
                self.recordregistry.remove(identifier)
                del self.sizeregistry[identifier]
                os.remove(path)
                print 'File ' + identifier + ' successfully removed.'
                return 0
            print 'File in use, could not be removed.'
            return -1
        print 'Could not find file ' + identifier + '.'
        return  -1
    
    def update(self, identifier, fp):
        """replace file data of stored file with new file data"""
        if self.search(identifier) is not -1:    
            self.remove(identifier)
            self.add(identifier, fp)
            print 'File ' + identifier + ' successfully updated.'
            return 0
        print 'Could not find file ' + identifier + '.'
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
        fp.seek(0)
        path = self.generate_file_handle(identifier)
        fp_to_disk = open(path, 'wb')
        tempstr = ""
        
        tempstr = fp.read(4096)
        while tempstr != "":
            fp_to_disk.write(tempstr)
            tempstr = fp.read(4096)
        
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
        except ValueError:
            print 'couldnt read this file bro.'
            return -1

    def empty_storage(self):
        while len(self.recordregistry) > 0:
            self.remove(self.recordregistry[0])

class MRUPreserve(Partition):
    """Nature preserve for unique, free-roaming most-recently-used files"""
    #but we shall quickly eradicate files we find unuseful and idle

    def __init__(self, path='/home/admin/accellion_filestore', capacity=2000):
        super(MRUPreserve, self).__init__(path, capacity)

    def assess_removals(self, identifier, file_size):
        """determines if we need to make space for new cache items"""
        #check size of file
        #if incoming size is greater than available space
        if file_size >= self.partremsize:
        #start kickin out old data
            print self.sizeregistry
            while file_size >= self.partremsize:
                currentid = self.recordregistry[ -1 ]
                self.remove(currentid)
        return 0
            
    def add(self, identifier, fp):
        """perform storage of file to disk"""
        #performs same function as add in Partition class, but
        #    actively removes files if filesize > space remaining
        
        file_size = os.fstat(fp.fileno())[6]
        self.sizeregistry[identifier] = file_size
        if file_size >= self.partsize:
            print 'File ' + identifier + ' too large to store. Capacity: ' + str(self.partsize) + 'bytes'
            return -1
        if file_size >= self.partremsize:
            self.assess_removals(identifier, file_size)
        if self.search(identifier) > -1:
            self.update(identifer, fp)
            return 0
        self.partremsize = self.partremsize - file_size
        self.write_disk(identifier, fp)
        self.recordregistry.insert(0, identifier)
        print 'inserting ' + identifier
        print 'File ' + identifier + ' successfully stored.'
        return 0

    def update(self, identifier, fp):
        """replace file data of stored file with new file data"""
        #unique to this method, returns a tuple of 0 return value and deleted objects
        if self.search(identifier) is not -1:    
            self.remove(identifier)
            self.add(identifier, fp)
            print 'File ' + identifier + ' successfully updated.'
            return 0
        print 'Could not find file ' + identifier + '.'
        return -1

