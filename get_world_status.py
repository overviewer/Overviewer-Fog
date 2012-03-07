#!/usr/bin/python2

import sys
import os
from pprint import pprint

try:
    import boto
except ImportError:
    sys.path.append(os.path.expanduser("~/devel/boto"))
    import boto


from boto.sdb.connection import SDBConnection 

uid = sys.argv[1]
print "Looking up", uid

sdb = SDBConnection()
db = sdb.get_domain("overviewerdb")
data = db.get_item(uid)
pprint(data)
