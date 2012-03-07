import sys
import os
import json

try:
    import boto
except ImportError:
    sys.path.append(os.path.expanduser("~/devel/boto"))
    import boto

from boto.sqs.connection import SQSConnection 
from boto.sdb.connection import SDBConnection 
from boto.sqs.message import Message


import uuid

if "AWS_ACCESS_KEY_ID" not in os.environ:
    print "Please set the AWS_ACCESS_KEY_ID environment variable"
if "AWS_SECRET_ACCESS_KEY" not in os.environ:
    print "Please set the AWS_SECRET_ACCESS_KEY environment variable"

def submit():
    if len(sys.argv) < 3:
        print "Usage:"
        print "  %s -seed <seed>" % sys.argv[0]
        return

    seed = sys.argv[2]
    print "Generate world with this seed (\"%s\") seed [y/N]?" % seed
    if raw_input().lower() == 'y':
        uid = uuid.uuid4()

        print "Submitting job %s to queue..." % uid
        sqs = SQSConnection()
        sdb = SDBConnection()
        queue = sqs.get_queue("overviewer-genfromseed")
        db = sdb.get_domain("overviewerdb")
        print queue
        print db

        data = dict()
        data['uuid'] = str(uid)
        data['seed'] = seed
        data['generated'] = False
        if not db.put_attributes(uid, data):
            print "***Error: Failed to update the db"
            return 1
        
        msg = Message()
        msg.set_body(str(uid))
        if not queue.write(msg):
            print "***Error: Failed to enqueue"
            return 1

        print "Ok, job enqueued"


    else:
        print "Ok, not submitting.  Bye"
        return
    

def generate():
    pass
    from boto.sqs.connection import SQSConnection 
    sqs = SQSConnection()

    queue = sqs.get_queue("overviewer-genfromseed")
    sdb = SDBConnection()
    db = sdb.get_domain("overviewerdb")

    message = queue.read(visibility_timeout=15)
    if not message:
        print "Nothing in the queue.  Please try again later"
        return 0
    uid = message.get_body()
    print "Got a job for %r" % uid
    data = db.get_item(uid)

    print data
    # this script generate maps from seeds
    # if this map is already generated, then upate the db
    if data['generated'] == 'True':
        print "---Warning: I was asked to generate this map, but it's already generated"
        print "            I'm going to update the db, but not re-generate"
        data['generated'] = True
        data.save()
        queue.delete_message(message)
        return 

    # TODO
    # Generate this map using a minecraft server
    # tar up the result
    # upload to S3
    print "TODO: generate map with seed %r" % data['seed']

if __name__ == "__main__":
    if "-seed" in sys.argv:
        submit()
    elif "-generate" in sys.argv:
        generate()
    else:
        print "Usage:"
        print "  %s -seed <seed>" % sys.argv[0]
        print "  %s -generate" % sys.argv[0]

