PROJECT(CouchDB)
CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
ADD_ERLANG_INCLUDE_DIR(${CMAKE_CURRENT_SOURCE_DIR}/src/couchdb)

SET(COUCHDB_RUNTEST ${CMAKE_CURRENT_SOURCE_DIR}/test/etap/runtest.py)
SET(COUCHDB_BIN_PATH ${CMAKE_CURRENT_BINARY_DIR})
SET(COUCHSTORE_BIN_PATH ${CMAKE_BINARY_DIR}/couchstore)
SET(abs_top_srcdir ${CMAKE_CURRENT_SOURCE_DIR})
SET(abs_top_builddir ${CMAKE_CURRENT_BINARY_DIR})

ADD_CUSTOM_TARGET(check COMMAND ${CMAKE_CTEST_COMMAND} -j4
                  DEPENDS dialyzer)

IF(WIN32)
  ADD_DEFINITIONS(-D__WIN32__)
ELSEIF (NOT "${CMAKE_SYSTEM_NAME}" STREQUAL "SunOS")
  SET(COUCHBASE_UNRESOLVED -Wl,-undefined -Wl,dynamic_lookup)
ENDIF(WIN32)

SET(ERLANG_COMPILE_FLAGS +debug_info -Werror
  +nowarn_deprecated_function +nowarn_deprecated_type)


EXECUTE_PROCESS(COMMAND git describe
                WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                OUTPUT_VARIABLE COUCHDB_VERSION
                ERROR_QUIET
                OUTPUT_STRIP_TRAILING_WHITESPACE)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/test/python/set_view/run.py.tpl
               ${CMAKE_CURRENT_BINARY_DIR}/test/python/set_view/run.py)

SET(CMAKE_ERL_LIB_INSTALL_PREFIX "lib/couchdb/erlang/lib" CACHE STRING
  "Installation prefix for CouchDB libs")
MARK_AS_ADVANCED(CMAKE_ERL_LIB_INSTALL_PREFIX)

ADD_SUBDIRECTORY(bin)
ADD_SUBDIRECTORY(etc)
ADD_SUBDIRECTORY(src)
ADD_SUBDIRECTORY(share)

# Build the directory layout
FILE(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/tmp/lib
                    ${CMAKE_CURRENT_BINARY_DIR}/tmp/log
                    ${CMAKE_CURRENT_BINARY_DIR}/tmp/run/couchdb)

ADD_SUBDIRECTORY(test)


SET(COUCHDB_PLT_FILE ${CMAKE_CURRENT_BINARY_DIR}/couchdb.plt)
ADD_CUSTOM_TARGET(couchdb-check-plt
       COMMAND
           ${CMAKE_COMMAND}
               -DPLT_FILE=${COUCHDB_PLT_FILE}
               -P ${CMAKE_MODULE_PATH}/ErlangCheckPlt.cmake)

ADD_CUSTOM_COMMAND(OUTPUT ${COUCHDB_PLT_FILE}
       COMMAND
          dialyzer --output_plt ${COUCHDB_PLT_FILE}
                   --build_plt
                   -pa src/lhttpc
                   -pa src/couchdb
                   -pa src/couch_set_view
                   -pa src/couch_index_merger
                   -pa src/couch_view_parser
                   -pa src/mapreduce
                   -pa src/couch_dcp
                   --apps
                        compiler
                        crypto
                        erts
                        inets
                        kernel
                        os_mon
                        sasl
                        ssl
                        stdlib
                        xmerl
                        syntax_tools
                        public_key
                        tools
                        hipe
                        runtime_tools
                   -r
                        src/erlang-oauth
                        src/etap
                        src/mochiweb
                        src/snappy
                  VERBATIM )

ADD_CUSTOM_TARGET(dialyzer
       COMMAND
          dialyzer --plt ${COUCHDB_PLT_FILE}
                   --verbose
                   -Wno_improper_lists
                   -pa src/couchdb
                   -pa src/couch_set_view
                   -pa src/couch_index_merger
                   -pa src/couch_view_parser
                   -pa src/mapreduce
                   -pa src/lhttpc
                   -pa src/snappy
                   -pa src/couch_dcp
                   -r
                        src/couchdb
                        src/couch_set_view
                        src/couch_index_merger/ebin
                        src/couch_view_parser
                        src/snappy
                        src/lhttpc
                        src/mapreduce
                        src/couch_dcp
       DEPENDS
          couchdb-check-plt
          ${COUCHDB_PLT_FILE})
