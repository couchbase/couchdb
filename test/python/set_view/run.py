#!/usr/bin/python

import sys
import unittest

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


def main():
    if sys.version_info < (2, 7):
        print "You need Python 2.7 or higher."
        sys.exit(1)

    suite = unittest.TestSuite()

    suite.addTest(unittest.makeSuite(TestIncludeDocs))
    suite.addTest(unittest.makeSuite(TestStale))
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
    suite.addTest(unittest.makeSuite(TestErlangViews))

    unittest.TextTestRunner(verbosity = 2, failfast = True).run(suite)


main()
