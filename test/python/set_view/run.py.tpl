#!/usr/bin/env python

import sys
sys.path.insert(0, "%abs_top_builddir%/test/python/lib")
sys.path.insert(0, "%abs_top_builddir%/test/python/set_view")
sys.path.insert(0, "%abs_top_builddir%/test/python/set_view/common")
import unittest
from subprocess import call
from time import sleep

# set view test files
from include_docs import TestIncludeDocs
from stale import TestStale
from updates import TestUpdates
from passive_partitions import TestPassivePartitions
from passive_partitions_update import TestPassivePartitionsUpdate
from cleanup import TestCleanup
from compaction import TestCompaction
from compaction_transitions import TestCompactionTransitions
from update_cleanup import TestUpdateCleanup
from filter_partitions import TestFilterPartitions
from view_merge import TestViewMerge
from burst_state_updates import TestBurstStateUpdates
from many_partitions import TestManyPartitions
from erlang_views import TestErlangViews
from replica_index import TestReplicaIndex
from view_params import TestViewParams


def stop_couch():
    call(["%abs_top_builddir%/utils/run", "-d"])

def start_couch():
    stop_couch()
    call(["%abs_top_builddir%/utils/run", "-b"])
    sleep(5)


def main():
    print "\nStarting set view tests\n"
    suite = unittest.TestSuite()

    suite.addTest(unittest.makeSuite(TestIncludeDocs))
    suite.addTest(unittest.makeSuite(TestStale))
    suite.addTest(unittest.makeSuite(TestViewParams))
    suite.addTest(unittest.makeSuite(TestUpdates))
    suite.addTest(unittest.makeSuite(TestManyPartitions))
    suite.addTest(unittest.makeSuite(TestPassivePartitions))
    suite.addTest(unittest.makeSuite(TestPassivePartitionsUpdate))
    suite.addTest(unittest.makeSuite(TestCleanup))
    suite.addTest(unittest.makeSuite(TestCompaction))
    suite.addTest(unittest.makeSuite(TestCompactionTransitions))
    suite.addTest(unittest.makeSuite(TestUpdateCleanup))
    suite.addTest(unittest.makeSuite(TestFilterPartitions))
    suite.addTest(unittest.makeSuite(TestViewMerge))
    suite.addTest(unittest.makeSuite(TestBurstStateUpdates))
    # Only JavaScript is supported at the moment (and actually faster
    # than erlang views).
    # suite.addTest(unittest.makeSuite(TestErlangViews))
    suite.addTest(unittest.makeSuite(TestReplicaIndex))

    start_couch()
    result = None

    try:
        result = unittest.TextTestRunner(verbosity = 2).run(suite)
    finally:
        stop_couch()

    print "\nFinished execution of the set view tests\n"
    sys.exit(len(result.failures) + len(result.errors))


main()
