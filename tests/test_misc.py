#!/usr/bin/python2


import sys
sys.path.append(".")
from inspect import isfunction, isclass, ismethod


def test_docstrings():
    "Validate that all functions have docstrings"

    print "test"

    m = __import__("jobs")
    for thing in m.__all__:
        thething = getattr(m, thing)
        assert thething.__doc__ is not None, \
                repr(thing) + " is missing a doc string"

        if isclass(thething):  # we also want to check class members:
            print "checking", repr(thething), "is a class"
            for mthing in dir(thething):
                themthing = getattr(thething, mthing)
                if isfunction(themthing) or ismethod(themthing):
                    assert themthing.__doc__ is not None, \
                            repr(themthing) + " is missing a doc string"
