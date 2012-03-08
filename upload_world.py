#!/usr/bin/python2

import sys
import os
import uuid
import tempfile
import subprocess
import stat

try:
    import boto
except ImportError:
    sys.path.append(os.path.expanduser("~/devel/boto"))
    import boto

from boto.sdb.connection import SDBConnection 
from boto.s3.connection import S3Connection
from boto.s3.key import Key

def add_from_url():
    if len(sys.argv) < 3:
        print "Usage:"
        print "  %s -url <url>" % sys.argv[0]
        return

    url = sys.argv[2]
    print "Generate world with this url (\"%s\") [y/N]?" % url
    if raw_input().lower() != 'y':
        print "Ok, nevermind"
        return
    
    uid = uuid.uuid4()
    data = dict()
    data['uuid'] = uid
    data['world_url'] = url

    sdb = SDBConnection()
    db = sdb.get_domain("overviewerdb")
    if not db.put_attributes(uid, data):
        print "***Error: Failed to update the db"
        return 1
    print "Ok. DB updated"
    print uid

def add_from_path():
    if len(sys.argv) < 3:
        print "Usage:"
        print "  %s -path <path>" % sys.argv[0]
        return
    
    path = sys.argv[2]

    if os.path.isdir(path):
        print "You've specified a directory.  I'll tar it up before uploading"
        print "OK? [y/N] ", 
        if raw_input().lower() != 'y':
            print "Ok, nevermind."
            return

        tmpdir = tempfile.mkdtemp(prefix="mc_gen")
        print "tmpdir is", tmpdir
        print "Making tarball..."
        p = subprocess.Popen(["tar", "-cf", os.path.join(tmpdir, "world.tar"), "."],
                cwd=path)
        p.wait()
        if p.returncode != 0:
            print "***Error: tar failed"
            return

        print "OK."
        print "Compressing..."
        p = subprocess.Popen(["bzip2", "world.tar"],
                shell=False,
                cwd=tmpdir)
        p.wait()
        if p.returncode != 0:
            print "***Error: compress failed"
            return
        print "OK."

        print "Checking filesize..."
        s = os.stat(os.path.join(tmpdir, "world.tar.bz2"))
        if s.st_size > 10*1024*1024:
            print "***Error: Compressed world is too big"
            return 1
        print "OK."


        uid = uuid.uuid4()
        print uid
        s3 = S3Connection()
        bucket = s3.get_bucket("overviewer-worlds")
        k = Key(bucket)
        k.key = "%s.tar.bz2" % uid
        print "Uploading to S3..."
        k.set_contents_from_filename(os.path.join(tmpdir, "world.tar.bz2"), reduced_redundancy=True)
        print "OK."
        k.make_public()

        urlbase = "https://s3.amazonaws.com/overviewer-worlds/"
        url = urlbase + k.key
        print "World is now available at:", url
   
        data = dict()
        data['uuid'] = uid
        data['world_url'] = url
        sdb = SDBConnection()
        db = sdb.get_domain("overviewerdb")
        if not db.put_attributes(uid, data):
            print "***Error: Failed to update the db"
            return 1
        print "Ok. DB updated"
    elif os.path.isfile(path):
        print "You've specified a file. I'll upload it without modification"
        print "OK? [y/N] ", 
        if raw_input().lower() != 'y':
            print "Ok, nevermind."
            return
        
        uid = uuid.uuid4()
        print uid
        s3 = S3Connection()
        bucket = s3.get_bucket("overviewer-worlds")
        k = Key(bucket)
        k.key = "%s.tar.bz2" % uid
        print "Uploading to S3..."
        k.set_contents_from_filename(path, reduced_redundancy=True)
        print "OK."
        k.make_public()

        urlbase = "https://s3.amazonaws.com/overviewer-worlds/"
        url = urlbase + k.key
        print "World is now available at:", url
   
        data = dict()
        data['uuid'] = uid
        data['world_url'] = url
        sdb = SDBConnection()
        db = sdb.get_domain("overviewerdb")
        if not db.put_attributes(uid, data):
            print "***Error: Failed to update the db"
            return 1
        print "Ok. DB updated"
    else:
        print "Sorry, I can't find that."
        return 1

if __name__ == "__main__":
    if "-url" in sys.argv:
        add_from_url()
    elif "-path" in sys.argv:
        add_from_path()
    else:
        print "Usage:"
        print "  %s -url <url>" % sys.argv[0]
        print "  %s -path <path>" % sys.argv[0]

