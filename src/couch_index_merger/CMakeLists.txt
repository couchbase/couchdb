FILE(GLOB SRC src/*.erl)
ADD_ERLANG_INCLUDE_DIR(${CMAKE_CURRENT_SOURCE_DIR}/../../src)
ADD_ERLANG_INCLUDE_DIR(${CMAKE_CURRENT_SOURCE_DIR}/../../src/couchdb)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/src/couch_index_merger.app.src
               ${CMAKE_CURRENT_BINARY_DIR}/ebin/couch_index_merger.app)

ERL_BUILD_OTP("couch_index_merger" ${SRC})
SET(COUCH_INDEX_MERGER_PREFIX ${CMAKE_ERL_LIB_INSTALL_PREFIX}/couch_index_merger-1.0.0)

SET(COUCH_INDEX_MERGER_INCLUDE_FILES include/couch_index_merger.hrl
                                     include/couch_view_merger.hrl)

INSTALL(FILES ${COUCH_INDEX_MERGER_INCLUDE_FILES}
       DESTINATION ${COUCH_INDEX_MERGER_PREFIX}/include)

INSTALL(FILES ${outfiles}
              ${CMAKE_CURRENT_BINARY_DIR}/ebin/couch_index_merger.app
        DESTINATION ${COUCH_INDEX_MERGER_PREFIX}/ebin)
