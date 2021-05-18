CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/run.tpl
               ${CMAKE_CURRENT_BINARY_DIR}/run)

SET(TEST_FILES
  00-setup.t
  01-node_to_node_encryption_test.t)


FOREACH (it ${TEST_FILES})
   GET_FILENAME_COMPONENT(testname ${it} NAME_WE)
   GET_FILENAME_COMPONENT(fullpath ${it} REALPATH)
   ADD_TEST(functional_test-${testname}
     ${COUCHDB_RUNTEST} -c ${COUCHSTORE_BIN_PATH} -p ${COUCHDB_BIN_PATH}/src
      -e ${ESCRIPT_EXECUTABLE} -t ${fullpath} --verbose)
ENDFOREACH (it)

ERL_BUILD(APPNAME "bucket_ops" SOURCES bucket_ops.erl)
ERL_BUILD(APPNAME "cluster_ops" SOURCES cluster_ops.erl)
