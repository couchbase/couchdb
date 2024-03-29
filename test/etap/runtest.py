#!/usr/bin/env python3
# Description: Wrapper script for running CouchDB unit tests

import sys
import getopt
import os
import subprocess
import re
import platform
import shlex

TEST_COUNT_RE = r"^1\.\.(\d+)"
TEST_OK_RE = r"(^ok)\b"
TEST_NOT_OK_RE = r"(^not ok)\b"

MODULES = ["../test/etap"]

def usage():
    print("Usage: %s -p builddir -l erl_libs_dir -m module_dir_paths" \
    " -f erl_flags -t testfile [ -v ] [ -c couchstore_install_path ]" \
    " [-e escript_path]" \
    % sys.argv[0])

def setup():
    """Configure LD_LIBRARY_PATH"""
    if platform.system() == "Linux":
        dname, fname = os.path.split(os.path.abspath(__file__))
        cpath = dname.rsplit('couchdb')[0]
        nspath = os.path.join(cpath, 'ns_server')

        def read_configuration():
            cfig = os.path.join(nspath, 'build', 'cluster_run.configuration')
            with open(cfig) as f:
                def fn(line):
                    k, v = line.strip().split('=')
                    return k, shlex.split(v)[0]
                return dict(fn(line) for line in f.readlines())

        config = read_configuration()
        PREFIX = config['prefix']

        LIBS = os.path.join(PREFIX, 'lib')
        MEMCACHED = os.path.join(LIBS, 'memcached')
        LD_LIBRARY_PATH = LIBS + os.pathsep + MEMCACHED
        env = os.environ.copy()
        if 'LD_LIBRARY_PATH' in env:
            LD_LIBRARY_PATH += os.pathsep + env['LD_LIBRARY_PATH']
        os.environ['LD_LIBRARY_PATH'] = LD_LIBRARY_PATH

def run_test(testfile,eescript_path, verbose = False):
    test_total = -1
    test_passed = 0
    exit_status = 0
    count_re = re.compile(TEST_COUNT_RE)
    ok_re = re.compile(TEST_OK_RE)
    not_ok_re = re.compile(TEST_NOT_OK_RE)
    s = subprocess.Popen([escript_path, testfile], stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
    while True:
        line = s.stdout.readline().decode('utf-8')
        if line:
            if ok_re.match(line):
                test_passed += 1
                print(line, end=''),
            elif not_ok_re.match(line):
                print(line, end=''),
            else:
                if verbose:
                    print(line, end=''),

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
            print(err, end='')

    if test_total > 0:
        print("%d/%d tests passed" %(test_passed, test_total))

    return exit_status

if __name__ == '__main__':
    try:
        opts, args = getopt.getopt(sys.argv[1:], "p:l:m:f:t:hvc:e:", \
                    ["path=", "libsdir=", "modules=", "flags=", "test=",
                     "help", "verbose", "couchstore-installdir=", "escript="])
    except (getopt.GetoptError, err):
        print(err)
        usage()
        sys.exit(2)

    if not opts:
        usage()
        sys.exit(2)

    path = None
    libsdir = None
    test = None
    modules = None
    flags = None
    verbose = False
    couchstore_path = None
    escript_path = 'escript'

    for opt, arg in opts:
        if opt in ("-p", "--path"):
            path = arg
        elif opt in ("-l", "--libsdir"):
            libsdir = arg
        elif opt in ("-m", "--modules"):
            modules = arg
        elif opt in ("-f", "--flags"):
            flags = arg
        elif opt in ("-t", "--test"):
            test = arg
        elif opt in ("-v", "--verbose"):
            verbose = True
        elif opt in ("-c", "--couchstore-installdir"):
            couchstore_path = arg
        elif opt in ("-e", "--escript"):
            escript_path = arg
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

    erl_flags = []
    env = os.getenv("ERL_FLAGS")
    if env:
        erl_flags.append(env)
    if flags:
        erl_flags.append(flags)

    if path:
        erl_libs.append(path)

        # As some modules are not proper OTP apps, just add all dirs as
        # ERL_FLAGS paths as well
        MODULES += [d for d in os.listdir(path)
                   if os.path.isdir(os.path.join(path, d))]

        paths = map(lambda x: " %s" %os.path.join(path, x), MODULES)
        erl_flags.append("-pa" + " ".join(paths))


    # Erlang modules expect posix path format
    # which is respected by all platforms
    # including Windows. So replace '\' by '/'
    # Ensure spaces escaped by '\' are intact,
    # since directory names might contain space
    pattern = re.compile(r'\\(\S)')

    erl_flags = " ".join(erl_flags)
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
        """
        Uncomment to fix mapreduce_nif_not_loaded
        setup()
        """
        sys.exit(run_test(test, escript_path, verbose))
    else:
        sys.exit("ERROR: No test specified")
