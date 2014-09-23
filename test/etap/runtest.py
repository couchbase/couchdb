#!/usr/bin/env python
# Description: Wrapper script for running CouchDB unit tests

import sys
import getopt
import os
import subprocess
import re

TEST_COUNT_RE = r"^1\.\.(\d+)"
TEST_OK_RE = r"(^ok)\b"
TEST_NOT_OK_RE = r"(^not ok)\b"

MODULES = ["../test/etap"]

def usage():
    print "Usage: %s -p builddir -l erl_libs_dir -m module_dir_paths" \
    " -t testfile [ -v ] [ -c couchstore_install_path ]" %sys.argv[0]
    print

def run_test(testfile, verbose = False):
    test_total = -1
    test_passed = 0
    exit_status = 0
    count_re = re.compile(TEST_COUNT_RE)
    ok_re = re.compile(TEST_OK_RE)
    not_ok_re = re.compile(TEST_NOT_OK_RE)
    s = subprocess.Popen(['escript', testfile], stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
    while True:
        line = s.stdout.readline()
        if line:
            if ok_re.match(line):
                test_passed += 1
                print line,
            elif not_ok_re.match(line):
                print line,
            else:
                if verbose:
                    print line,

                count = count_re.match(line)
                if count:
                    test_total = int(count.group(1))
        else:
            break

    if test_total == test_passed:
        exit_status = 0
    else:
        exit_status = 1
        err = s.stderr.read()
        if err:
            print err

    if test_total > 0:
        print "%d/%d tests passed" %(test_passed, test_total)

    return exit_status

if __name__ == '__main__':
    try:
        opts, args = getopt.getopt(sys.argv[1:], "p:l:m:t:hvc:", \
                    ["path=", "libsdir=", "modules=", "test=", "help",
                     "verbose", "couchstore-installdir="])
    except getopt.GetoptError, err:
        print err
        usage()
        sys.exit(2)

    if not opts:
        usage()
        sys.exit(2)

    path = None
    libsdir = None
    test = None
    modules = None
    verbose = False
    couchstore_path = None

    for opt, arg in opts:
        if opt in ("-p", "--path"):
            path = arg
        if opt in ("-l", "--libsdir"):
            libsdir = arg
        if opt in ("-m", "--modules"):
            modules = arg
        elif opt in ("-t", "--test"):
            test = arg
        elif opt in ("-v", "--verbose"):
            verbose = True
        elif opt in ("-c", "--couchstore-installdir"):
            couchstore_path = arg
        elif opt in ("-h", "--help"):
            usage()
            sys.exit(0)

    if modules:
        MODULES += modules.split(",")

    erl_libs = []
    env = os.getenv("ERL_LIBS")
    if env:
        erl_libs.append(env)

    if libsdir:
        erl_libs.append(libsdir)

    # Erlang modules expect posix path format
    # which is respected by all platforms
    # including Windows. So replace '\' by '/'
    # Ensure spaces escaped by '\' are intact,
    # since directory names might contain space
    pattern = re.compile(r'\\(\S)')

    erl_flags = ""
    env = os.getenv("ERL_FLAGS")
    if env:
        erl_flags = env

    if path:
        erl_libs.append(path)

        # As some modules are not proper OTP apps, just add all dirs as
        # ERL_FLAGS paths as well
        MODULES += [d for d in os.listdir(path)
                   if os.path.isdir(os.path.join(path, d))]

        flags = map(lambda x: " %s" %os.path.join(path, x), MODULES)
        erl_flags = "-pa" + " ".join(flags)
        env = os.getenv("ERL_FLAGS")
        erl_flags += " -pa" + " ".join(flags)
        erl_flags = pattern.sub(r'/\1', erl_flags)
        os.putenv("ERL_FLAGS", erl_flags)

    erl_libs = os.pathsep.join(erl_libs)
    erl_libs = pattern.sub(r'/\1', erl_libs)
    os.putenv("ERL_LIBS", erl_libs)

    if verbose:
        print('ERL_LIBS="{}"'.format(erl_libs))
        print('ERL_FLAGS="{}"'.format(erl_flags))

    if couchstore_path:
        env = os.getenv("PATH")
        if env:
            env += ":"
        else:
            env = ""

        env += couchstore_path
        os.putenv("PATH", env)

    if test:
        sys.exit(run_test(test, verbose))
    else:
        sys.exit("ERROR: No test specified")
