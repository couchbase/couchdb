SET(COUCH_VIEW_SNAPPY_SRC src/snappy_nif.cc)
SET(COUCH_SNAPPY_VERSION "1.0.4")
SET(COUCH_SNAPPY_PREFIX ${CMAKE_ERL_LIB_INSTALL_PREFIX}/snappy-${COUCH_SNAPPY_VERSION})
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

IF (UNIX)
    # It would be nice to share this code from
    # tlm/cmake/Modules/CouchbaseRpath.cmake, but unfortunately MacOS has
    # a distinction between @loader_path and @executable_path that doesn't
    # map to anything on other Unices, so it just has to be special
    IF (APPLE)
        SET (ORIGIN @loader_path)
    ELSE ()
        SET (ORIGIN \$ORIGIN)
    ENDIF ()
    # map from lib/couchdb/erlang/lib/snappy-1.0.4/priv/ to lib/
    SET_TARGET_PROPERTIES(snappy_nif PROPERTIES
                          INSTALL_RPATH "${ORIGIN}/../../../../..")
ENDIF ()

INSTALL(TARGETS snappy_nif
        DESTINATION ${COUCH_SNAPPY_PRIV_PREFIX})

INSTALL(DIRECTORY ${COUCHDB_DEFAULT_LIB_DIR}/snappy/ebin
        DESTINATION ${COUCH_SNAPPY_PREFIX})
