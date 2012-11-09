#!/usr/bin/python2

"""
Helper utility to make it easier to manage AWS credentials.  Run with the
-i option to python


Create a config.ini file like this:

    [credentials]
    bob_accesskey=xxyyzz
    bob_secretkey=112233

    test_accesskey=uuiioo
    test_secretkey=445566

You can then use --user=bob or --user=test to automatically
set the AWS credentials

sqs, sdb, and s3 connections are automatically made and ready
to use

"""

import boto
import os
import sys
from ConfigParser import SafeConfigParser as configParser
from optparse import OptionParser

parser = OptionParser()
parser.add_option("--config", default="config.ini")
parser.add_option("--user")

(options, args) = parser.parse_args()


ini = configParser()
ini.read(options.config)


def setCredentials(username):
    if not ini.has_option("credentials", username + "_accesskey"):
        return False

    os.environ['AWS_ACCESS_KEY_ID'] = ini.get("credentials", username + "_accesskey")
    os.environ['AWS_SECRET_ACCESS_KEY'] = ini.get("credentials", username + "_secretkey")
    print "Using credentials for", username
    return True

if options.user:
    if not setCredentials(options.user):
        print "No such user in your config file"
        os.environ['PYTHONINSPECT'] = ''
        sys.exit(1)
elif 'AWS_ACCESS_KEY_ID' not in os.environ:
    if not setCredentials(os.getlogin()) and not setCredentials("default"):
        print "Unable to find any credentials to use"

else:
    print "Using credentials from your environment"

print "The following variables have been created:"
connections = ["sqs", "sdb", "s3"]
for conn in connections:
    locals()[conn] = globals()[conn] = getattr(boto, "connect_" + conn)()
    print " *", conn
