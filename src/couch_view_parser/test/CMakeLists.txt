SET(TEST_FILES
  01-map-view.t
  02-reduce-view.t)

FOREACH (it ${TEST_FILES})
   GET_FILENAME_COMPONENT(testname ${it} NAME_WE)
   GET_FILENAME_COMPONENT(fullpath ${it} REALPATH)
   ADD_TEST(couchdb-couch_view_parser-${testname}
     ${COUCHDB_RUNTEST} -c ${COUCHSTORE_BIN_PATH} -p ${COUCHDB_BIN_PATH}/src
     -e ${ESCRIPT_EXECUTABLE} -e ${ESCRIPT_EXECUTABLE} -t ${fullpath} --verbose)
ENDFOREACH (it)
