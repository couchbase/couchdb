SET(TEST_FILES
  01-map.t
  02-reduce.t
  03-builtin-functions.t)

FOREACH (it ${TEST_FILES})
   GET_FILENAME_COMPONENT(testname ${it} NAME_WE)
   GET_FILENAME_COMPONENT(fullpath ${it} REALPATH)
   ADD_TEST(couchdb-mapreduce-${testname} ${PYTHON_EXECUTABLE}
     ${COUCHDB_RUNTEST} -c ${COUCHSTORE_BIN_PATH} -p ${COUCHDB_BIN_PATH}/src
     -e ${ESCRIPT_EXECUTABLE} -t ${fullpath} --verbose)
ENDFOREACH (it)
