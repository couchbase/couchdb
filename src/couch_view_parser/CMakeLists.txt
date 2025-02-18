SET(COUCH_VIEW_PARSER_VERSION "1.0")
SET(COUCH_VIEW_PARSER_PREFIX ${CMAKE_ERL_LIB_INSTALL_PREFIX}/couch_view_parser-${COUCH_VIEW_PARSER_VERSION})
SET(COUCH_VIEW_PARSER_PRIV_PREFIX ${COUCH_VIEW_PARSER_PREFIX}/priv)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/test/run.tpl
               ${CMAKE_CURRENT_BINARY_DIR}/test/run)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/src/couch_view_parser.app.src
               ${CMAKE_CURRENT_BINARY_DIR}/couch_view_parser.app)


SET(COUCH_VIEW_YAJL_SRC couch_view_parser.cc
    couch_view_parser_nif.cc
    yajl/yajl.c
    yajl/yajl_alloc.c
    yajl/yajl_buf.c
    yajl/yajl_encode.c
    yajl/yajl_gen.c
    yajl/yajl_lex.c
    yajl/yajl_parser.c
    yajl/yajl_tree.c
    yajl/yajl_version.c)

INCLUDE_DIRECTORIES(BEFORE ${CMAKE_CURRENT_SOURCE_DIR}
                           ${Platform_SOURCE_DIR}/include
                           ${ERLANG_INCLUDE_PATH})



ADD_LIBRARY(couch_view_parser_nif MODULE ${COUCH_VIEW_YAJL_SRC})
SET_TARGET_PROPERTIES(couch_view_parser_nif PROPERTIES PREFIX "")

TARGET_LINK_LIBRARIES(couch_view_parser_nif ${COUCHBASE_UNRESOLVED}
                                   ${COUCHBASE_MATH_LIBS} platform_cbassert_unsanitized)
SET_TARGET_PROPERTIES(couch_view_parser_nif PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/priv"
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/priv"
)
# couch_view_parser_nif is loaded into Erlang VM (beam.smp) which
# doesn't link the sanitizer libs and hence cannot successfully load
# couch_view_parser_nif if it has the sanitizers enabled. As such
# disable them.
remove_sanitizers(couch_view_parser_nif)

INSTALL(TARGETS couch_view_parser_nif
        DESTINATION ${COUCH_VIEW_PARSER_PRIV_PREFIX})
INSTALL(DIRECTORY ${COUCHDB_DEFAULT_LIB_DIR}/couch_view_parser/ebin
        DESTINATION ${COUCH_VIEW_PARSER_PREFIX})

ADD_SUBDIRECTORY(test)
