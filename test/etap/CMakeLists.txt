FILE(GLOB TEST_FILES *.t)
LIST(SORT TEST_FILES)

FOREACH (it ${TEST_FILES})
   GET_FILENAME_COMPONENT(testname ${it} NAME_WE)
   ADD_TEST(couchdb-etap-${testname} ${PYTHON_EXECUTABLE} ${COUCHDB_RUNTEST} -c ${COUCHSTORE_BIN_PATH} -p ${COUCHDB_BIN_PATH} -s ${CouchDB_SOURCE_DIR} -t ${it})
ENDFOREACH (it)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/test_util.erl.in
               ${CMAKE_CURRENT_BINARY_DIR}/test_util.erl)


CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/run.tpl
               ${CMAKE_CURRENT_BINARY_DIR}/run)

FILE(GLOB ETAP_SRC *.erl)
ERL_BUILD("etap_test" ${ETAP_SRC} ${CMAKE_CURRENT_BINARY_DIR}/test_util.erl)