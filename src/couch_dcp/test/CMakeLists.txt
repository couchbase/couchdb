CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/run.tpl
               ${CMAKE_CURRENT_BINARY_DIR}/run)

SET(TEST_FILES
  00-prepare.t
  01-load.t
  04-view-groups.t
  05-couch-dcp-client.t
  06-rollback.t)

FOREACH (it ${TEST_FILES})
   GET_FILENAME_COMPONENT(testname ${it} NAME_WE)
   GET_FILENAME_COMPONENT(fullpath ${it} REALPATH)
   ADD_TEST(couchdb-couch_dcp-${testname} python3
     ${COUCHDB_RUNTEST} -c ${CMAKE_INSTALL_PREFIX}/bin -p ${COUCHDB_BIN_PATH}/src
     -m couch_set_view/test -e ${ESCRIPT_EXECUTABLE} -t ${fullpath} --verbose)
ENDFOREACH (it)
