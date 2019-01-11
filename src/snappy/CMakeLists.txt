CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/snappy.app.in
               ${CMAKE_CURRENT_BINARY_DIR}/snappy.app)


SET(COUCH_VIEW_SNAPPY_SRC snappy_nif.cc)
SET(COUCH_SNAPPY_VERSION "1.0.4")
SET(COUCH_SNAPPY_PREFIX ${CMAKE_ERL_LIB_INSTALL_PREFIX}/snappy-${COUCH_SNAPPY_VERSION})
SET(COUCH_SNAPPY_EBIN_PREFIX ${COUCH_SNAPPY_PREFIX}/ebin)
SET(COUCH_SNAPPY_PRIV_PREFIX ${COUCH_SNAPPY_PREFIX}/priv)

INCLUDE_DIRECTORIES(BEFORE ${SNAPPY_INCLUDE_DIR}
                           ${ERLANG_INCLUDE_PATH})

ADD_LIBRARY(snappy_nif MODULE ${COUCH_VIEW_SNAPPY_SRC})
SET_TARGET_PROPERTIES(snappy_nif PROPERTIES PREFIX "")
TARGET_LINK_LIBRARIES(snappy_nif ${COUCHBASE_UNRESOLVED}
                                 ${SNAPPY_LIBRARIES}
                                 ${COUCHBASE_MATH_LIBS})

SET_TARGET_PROPERTIES(snappy_nif PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/priv"
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/priv"
)
# snappy_nif is loaded into Erlang VM (beam.smp) which doesn't link
# the sanitizer libs and hence cannot successfully load snappy_nif if
# it has the sanitizers enabled. As such disable them.
remove_sanitizers(snappy_nif)

IF (APPLE)
    # map from lib/couchdb/erlang/lib/snappy-1.0.4/priv/ to lib/
    SET_TARGET_PROPERTIES(snappy_nif PROPERTIES
                          INSTALL_RPATH "@loader_path/../../../../..")
ENDIF ()

ERL_BUILD(APPNAME "snappy" SOURCES snappy.erl)

INSTALL(TARGETS snappy_nif
        DESTINATION ${COUCH_SNAPPY_PRIV_PREFIX})

INSTALL(FILES ${outfiles}
              ${CMAKE_CURRENT_BINARY_DIR}/snappy.app
        DESTINATION ${COUCH_SNAPPY_EBIN_PREFIX})
