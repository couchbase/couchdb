CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/run.tpl
               ${CMAKE_CURRENT_BINARY_DIR}/run)

SET(TEST_FILES
  00-prepare.t
  01-collation.t
  01-headers.t
  01-load.t
  #02-old-index-cleanup.t
  03-db-compaction-file-leaks.t
  05-replicas-transfer.t
  06-main-compaction.t
  07-replica-compaction.t
  08-deletes-cleanup.t
  09-deletes-cleanup-many-views.t
  10-updates-cleanup.t
  11-updates-cleanup-many-views.t
  12-errors.t
  13-progressive-cleanup.t
  14-duplicated-keys-per-doc.t
  15-passive-partitions.t
  16-pending-transition.t
  17-unindexable-partitions.t
  18-monitor-partition-updates.t
  19-compaction-retry.t
  20-debug-params.t
  21-updater-cleanup.t
  22-compactor-cleanup.t
  23-replica-group-missing.t
  24-updater-add-more-passive-partitions.t
  25-util-stats.t
  26-multiple-reductions.t
  27-dev-views.t
  28-big-reduce.t
  29-rollback.t
  #30-query-fdleaks.t
  31-ryow-query.t
  32-upgrade.t
  33-dcp-duplicates.t
  34-truncate.t
  35-meta-params.t
  35-partition-versions.t
  36-write-guard.t)

FOREACH (it ${TEST_FILES})
   GET_FILENAME_COMPONENT(testname ${it} NAME_WE)
   GET_FILENAME_COMPONENT(fullpath ${it} REALPATH)
   ADD_TEST(couchdb-couch_set_view-${testname} ${PYTHON_EXECUTABLE}
     ${COUCHDB_RUNTEST} -c ${CMAKE_INSTALL_PREFIX}/bin -p ${COUCHDB_BIN_PATH}/src
     -m couch_set_view/test -e ${ESCRIPT_EXECUTABLE} -t ${fullpath} --verbose)
ENDFOREACH (it)

ERL_BUILD(APPNAME "couch_set_view_test" SOURCES couch_set_view_test_util.erl)
