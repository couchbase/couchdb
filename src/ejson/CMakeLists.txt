SET(EJSON_VERSION "0.1.0")
SET(EJSON_PREFIX ${CMAKE_ERL_LIB_INSTALL_PREFIX}/ejson-${EJSON_VERSION})
SET(EJSON_EBIN_PREFIX ${EJSON_PREFIX}/ebin)
SET(EJSON_PRIV_PREFIX ${EJSON_PREFIX}/priv)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/src/ejson.app.src
               ${CMAKE_CURRENT_BINARY_DIR}/ejson.app)

SET(EJSON_SRC src/ejson.c
              src/decode.c
              src/encode.c
              yajl/yajl_alloc.c
              yajl/yajl_buf.c
              yajl/yajl.c
              yajl/yajl_encode.c
              yajl/yajl_gen.c
              yajl/yajl_lex.c
              yajl/yajl_parser.c)

INCLUDE_DIRECTORIES(BEFORE ${CMAKE_CURRENT_SOURCE_DIR}
                           ${ERLANG_INCLUDE_PATH})

ADD_LIBRARY(ejson MODULE ${EJSON_SRC})
SET_TARGET_PROPERTIES(ejson PROPERTIES PREFIX "")
TARGET_LINK_LIBRARIES(ejson ${COUCHBASE_UNRESOLVED})
SET_TARGET_PROPERTIES(ejson PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY "${COUCHDB_DEFAULT_LIB_DIR}/ejson/priv"
    RUNTIME_OUTPUT_DIRECTORY "${COUCHDB_DEFAULT_LIB_DIR}/ejson/priv"
)
# ejson is loaded into Erlang VM (beam.smp) which doesn't
# link the sanitizer libs and hence cannot successfully
# load ejson if it has the sanitizers enabled. As such
# disable them.
remove_sanitizers(ejson)

INSTALL(TARGETS ejson
        DESTINATION ${EJSON_PRIV_PREFIX})
INSTALL (DIRECTORY ${COUCHDB_DEFAULT_LIB_DIR}/ejson/ebin DESTINATION
        ${EJSON_PREFIX})
