#!/usr/bin/python2

"""
This script is designed to be installed as an authorized_keys command.  For exmaple:

command="/usr/bin/python2 /home/upload/bin/do_upload.py",no-port-forwarding,no-pty ssh-rsa AAAAB...XXYYZZ achin@paphlagon

This allows you to distribute private keys to worker processes, and be assured that they can only execute this script

To upload a file, you might run a command like this:

    ssh -T -i privkey.rsa -l upload example.org do_upload -stuff here < file.tar.gz


There are two types of uploads, files and directories.
Files are simply copied over the ssh pipe and written directly to disk
Directores are streamed over the pipe as a [possibly compressed] tarbal,
and piped to tar -x

"""

import sys
import os
import traceback
import shutil
import subprocess


try:
    cmd = os.environ.get("SSH_ORIGINAL_COMMAND", "").split()
    sys.stderr.write("SSH_ORIGINAL_COMMMAND is %r\n" % cmd)

    if cmd[0] != "do_upload":
        raise Exception("bad command")

    if "-file" in cmd:
        inobj = sys.stdin
        if "-bzip2" in cmd:
            p = subprocess.Popen(["bzip2", "-dc"], stdin=sys.stdin, stdout=subprocess.PIPE)
            inobj = p.stdout
        with open("/tmp/incoming.dat", "w") as f:
            shutil.copyfileobj(inobj, f)
        if "-bzip2" in cmd:
            p.wait()
    elif "-dir" in cmd:

        args = []
        if "-bzip2" in cmd:
            args.append("-j")
        p = subprocess.Popen(["tar", "-C", "/tmp/incoming", "-x"] + args, stdin=sys.stdin, stdout=sys.stderr)
        p.wait()
    else:
        sys.stderr.write("what?\n")

    sys.stderr.write("World untarred and ready to be served\n")

except:
    sys.stderr.write("\nSomething went wrong!\n")
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)
sys.exit(0)
