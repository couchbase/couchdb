FILE(GLOB SRC *.erl)
SET(EJSON_VERSION "0.1.0")
SET(EJSON_PREFIX ${CMAKE_ERL_LIB_INSTALL_PREFIX}/ejson-${EJSON_VERSION})
SET(EJSON_EBIN_PREFIX ${EJSON_PREFIX}/ebin)
SET(EJSON_PRIV_PREFIX ${EJSON_PREFIX}/priv)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/ejson.app.in
               ${CMAKE_CURRENT_BINARY_DIR}/ejson.app)

SET(EJSON_SRC ejson.c
              decode.c
              encode.c
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
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/priv"
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/priv"
)



ERL_BUILD("ejson_build" ${SRC})

INSTALL(TARGETS ejson
        DESTINATION ${EJSON_PRIV_PREFIX})
INSTALL(FILES ${outfiles}
              ${CMAKE_CURRENT_BINARY_DIR}/ejson.app
        DESTINATION ${EJSON_EBIN_PREFIX})
