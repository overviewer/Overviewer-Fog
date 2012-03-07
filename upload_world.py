#!/usr/bin/python2

import sys
import os
import uuid

try:
    import boto
except ImportError:
    sys.path.append(os.path.expanduser("~/devel/boto"))
    import boto

from boto.sdb.connection import SDBConnection 

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


if __name__ == "__main__":
    if "-url" in sys.argv:
        add_from_url()
    elif "-path" in sys.argv:
        add_from_path()
    else:
        print "Usage:"
        print "  %s -url <url>" % sys.argv[0]
        print "  %s -path <path>" % sys.argv[0]

