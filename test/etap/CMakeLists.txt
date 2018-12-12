SET(TEST_FILES
  001-load.t
  002-write-guard.t
  010-file-basics.t
  011-file-headers.t
  020-btree-basics.t
  021-btree-reductions.t
  022-btree-copy.t
  023-btree-guided-purge.t
  024-btree-guided-fold.t
  030-doc-from-json.t
  031-doc-to-json.t
  032-doc-to-binary-views.t
  033-doc-from-binary.t
  040-util.t
  041-uuid-gen.t
  042-work-queue.t
  070-couch-db.t
  #071-couch-db-external-write.t
  073-changes.t
  080-config-get-set.t
  081-config-override.t
  082-config-register.t
  083-config-no-files.t
  090-task-status.t
  100-ref-counter.t
  150-invalid-view-seq.t
  190-json-stream-parse.t
  201-view-group-shutdown.t
  202-dev-view-group-shutdown.t)

FOREACH (it ${TEST_FILES})
   GET_FILENAME_COMPONENT(testname ${it} NAME_WE)
   GET_FILENAME_COMPONENT(fullpath ${it} REALPATH)
   ADD_TEST(couchdb-etap-${testname} ${PYTHON_EXECUTABLE} ${COUCHDB_RUNTEST}
     -c ${COUCHSTORE_BIN_PATH} -p ${COUCHDB_BIN_PATH}/src
     -e ${ESCRIPT_EXECUTABLE} -t ${fullpath} --verbose)
ENDFOREACH (it)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/test_util.erl.in
               ${CMAKE_CURRENT_BINARY_DIR}/test_util.erl)


CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/run.tpl
               ${CMAKE_CURRENT_BINARY_DIR}/run)

ERL_BUILD(APPNAME "etap_test"
          SOURCES test_web.erl misc.erl ${CMAKE_CURRENT_BINARY_DIR}/test_util.erl)
