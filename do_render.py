#!/usr/bin/python2

import sys
import os
import json
import tempfile
import subprocess
import shutil
import uuid
import urllib2

try:
    import boto
except ImportError:
    sys.path.append(os.path.expanduser("~/devel/boto"))
    import boto

from boto.sqs.connection import SQSConnection 
from boto.sdb.connection import SDBConnection 
from boto.s3.connection import S3Connection
from boto.sqs.message import Message
from boto.s3.key import Key

# provide your own config.py that holds useful
# configuration settings
import config


if "AWS_ACCESS_KEY_ID" not in os.environ:
    print "Please set the AWS_ACCESS_KEY_ID environment variable"
if "AWS_SECRET_ACCESS_KEY" not in os.environ:
    print "Please set the AWS_SECRET_ACCESS_KEY environment variable"


def submit():
    if len(sys.argv) < 3:
        print "Usage:"
        print "  %s -submit <world uuid>" % sys.argv[0]
        return

    sdb = SDBConnection()
    db = sdb.get_domain("overviewerdb")

    # TODO use less crappy command line parsing
    world_uuid = uuid.UUID(sys.argv[2])

    world_item = db.get_item(world_uuid)
    if not world_item:
        print "Can't find that world!"
        return 1

    print "Submit this world for rendering? [y/N]"
    if raw_input().lower() != 'y':
        return "Ok, nevermind."
        return 0

    from boto.sqs.connection import SQSConnection 
    sqs = SQSConnection()
    
    queue = sqs.get_queue("overviewer-render")
    
    render_uuid = uuid.uuid4()
    print "Render UUID:", render_uuid
    data = dict()
    data['uuid'] = str(render_uuid)
    data['rendered'] = False
    data['world_uuid'] = str(world_uuid)

    if not db.put_attributes(str(render_uuid), data):
        print "***Error: Failed to update the db"
        return 1

    msg = Message()
    msg.set_body(str(render_uuid))

    if not queue.write(msg):
        print "***Error: Failed to enqueue"
        return 1
    print "Ok, job enqueued"
    return 0

    

def render():
    
    sqs = SQSConnection()
    queue = sqs.get_queue("overviewer-render")
    sdb = SDBConnection()
    db = sdb.get_domain("overviewerdb")
    
    message = queue.read(visibility_timeout=15)
    if not message:
        print "Nothing in the queue.  Please try again later"
        return 0

    render_uuid = message.get_body()
    print "World uuid:", render_uuid
    
    render_item = db.get_item(str(render_uuid))
    if not render_item:
        print "***Error can't find a world with that UUID"
        return 1

    world_uuid = render_item.get("world_uuid")
    world_item = db.get_item(str(world_uuid))


    url = world_item.get("world_url", None)
    if not url:
        print "***Error: can't find worldurl"
        return 1

    print "Getting map..." 
    map_url = urllib2.urlopen(url)
    print "OK."

    tmpdir = tempfile.mkdtemp(prefix="mc_gen")
    fobj = open(os.path.join(tmpdir, "world.tar.bz2"), "w")
    print "Downloading map to %s..." % tmpdir
    shutil.copyfileobj(map_url, fobj)
    fobj.close()
    print "OK."



if __name__ == "__main__":
    if "-submit" in sys.argv:
        submit()
    elif "-render" in sys.argv:
        render()
    else:
        print "Usage:"
        print "  %s -submit <world uuid>" % sys.argv[0]
        print "  %s -generate" % sys.argv[0]

