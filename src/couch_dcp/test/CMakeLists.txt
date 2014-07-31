CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/run.tpl
               ${CMAKE_CURRENT_BINARY_DIR}/run)

FILE(GLOB TEST_FILES *.t)
LIST(SORT TEST_FILES)

FOREACH (it ${TEST_FILES})
   GET_FILENAME_COMPONENT(testname ${it} NAME_WE)
   ADD_TEST(couchdb-couch_dcp-${testname} ${PYTHON_EXECUTABLE} ${COUCHDB_RUNTEST} -c ${COUCHSTORE_BIN_PATH} -p ${COUCHDB_BIN_PATH} -s ${CouchDB_SOURCE_DIR} -m couch_dcp,couch_set_view,${COUCHDB_BIN_PATH}/src/couch_set_view/test -t ${it})
ENDFOREACH (it)
