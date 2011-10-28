#!/usr/bin/python

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


def main():
    suite = unittest.TestSuite()

    suite.addTest(unittest.makeSuite(TestIncludeDocs))
    suite.addTest(unittest.makeSuite(TestStale))
    suite.addTest(unittest.makeSuite(TestUpdates))
    suite.addTest(unittest.makeSuite(TestPassivePartitions))
    suite.addTest(unittest.makeSuite(TestPassivePartitionsUpdate))
    suite.addTest(unittest.makeSuite(TestCleanup))
    suite.addTest(unittest.makeSuite(TestCompaction))
    suite.addTest(unittest.makeSuite(TestCompactionTransitions))
    suite.addTest(unittest.makeSuite(TestUpdateCleanup))
    suite.addTest(unittest.makeSuite(TestFilterPartitions))
    suite.addTest(unittest.makeSuite(TestViewMerge))

    unittest.TextTestRunner(verbosity = 2, failfast = True).run(suite)


main()
