#!/usr/bin/python2

import sys
import os
import json
import tempfile
import subprocess
import shutil

try:
    import redstone # <3 libredstone
except ImportError:
    sys.path.append(os.path.expanduser("~/devel/libredstone/bindings"))
    import redstone

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

import uuid

if "AWS_ACCESS_KEY_ID" not in os.environ:
    print "Please set the AWS_ACCESS_KEY_ID environment variable"
if "AWS_SECRET_ACCESS_KEY" not in os.environ:
    print "Please set the AWS_SECRET_ACCESS_KEY environment variable"

def submit():
    if len(sys.argv) < 3:
        print "Usage:"
        print "  %s -seed <seed> [<spawn coords>]" % sys.argv[0]
        return

    # TODO use less crappy command line parsing
    seed = sys.argv[2]

    if len(sys.argv) == 4:
        spawn = [int(x) for x in sys.argv[3].split(",")]
        assert(len(spawn) == 3)
        print "Generate world with this seed (\"%s\") with spawn %r [y/N]?" % (seed, spawn)
    else:
        spawn = None
        print "Generate world with this seed (\"%s\") [y/N]?" % seed
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
        if spawn:
            data['target_spawn'] = spawn
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

    if 'target_spawn' in data:
        data['target_spawn'] = map(int, data['target_spawn'])
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


    # check config options
    if not os.path.isfile(config.minecraft_server):
        raise Exception("minecraft_server isn't configured")

    # with a generous amount of rounding up, assume it'll take 5 minutes to generate the map
    message.change_visibility(5*60)

    tmpdir = tempfile.mkdtemp(prefix="mc_gen")

    # create a settings.properties file with our seed
    with open(os.path.join(tmpdir, "server.properties"), "w") as f:
        f.write("level-seed=%s" % data['seed'])

    p = subprocess.Popen(["java", "-jar",
        config.minecraft_server, "-noGUI"],
        shell=False,
        stdin=subprocess.PIPE,
        cwd=tmpdir)

    p.stdin.write("stop\n")
    p.stdin.close()
    p.wait()
    print ""
    print "Minecraft server exited with %r" % p.returncode
    print "World resided in %r" % tmpdir

    # if we want a specific spawn, we need to rewrite the level.dat file,
    # remove the old region files, and restart the server
    if 'target_spawn' in data:
        s = data['target_spawn']
        leveldat = redstone.NBT.parse_from_file(os.path.join(tmpdir, "world", "level.dat"))
        root = leveldat.root
        root['Data']['SpawnX'].set_integer(int(s[0]))
        root['Data']['SpawnY'].set_integer(int(s[1]))
        root['Data']['SpawnZ'].set_integer(int(s[2]))
        leveldat.write_to_file(os.path.join(tmpdir, "world", "level.dat"))

        shutil.rmtree(os.path.join(tmpdir,"world","region"))
        p = subprocess.Popen(["java", "-jar",
            config.minecraft_server, "-noGUI"],
            shell=False,
            stdin=subprocess.PIPE,
            cwd=tmpdir)

        p.stdin.write("stop\n")
        p.stdin.close()
        p.wait()
        print "Minecraft server exited with %r" % p.returncode

    message.change_visibility(5*60)
    print "Making tarball..."
    p = subprocess.Popen(["tar", "-cf", "world.tar", "world/"],
        cwd=tmpdir)
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

    data['generated'] = True
    data['world_url'] = url
    data.save()
    print "Database updated."

    queue.delete_message(message)

    print "All done!"




if __name__ == "__main__":
    if "-seed" in sys.argv:
        submit()
    elif "-generate" in sys.argv:
        generate()
    else:
        print "Usage:"
        print "  %s -seed <seed> [<spawn coords>]" % sys.argv[0]
        print "  %s -generate" % sys.argv[0]

