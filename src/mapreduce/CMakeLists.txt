SET(MR_VERSION "1.0")
SET(MR_PREFIX ${CMAKE_ERL_LIB_INSTALL_PREFIX}/mapreduce-${MR_VERSION})
SET(MR_PRIV_PREFIX ${MR_PREFIX}/priv)


CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/test/run.tpl
               ${CMAKE_CURRENT_BINARY_DIR}/test/run)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/src/mapreduce.app.src
               ${CMAKE_CURRENT_BINARY_DIR}/mapreduce.app)



SET(COUCH_VIEW_MR_SRC mapreduce_nif.cc
                      mapreduce.cc
                      ${CMAKE_CURRENT_BINARY_DIR}/jsfunctions/jsfunctions_data.cc)

INCLUDE_DIRECTORIES(BEFORE ${V8_INCLUDE_DIR}
                           ${ERLANG_INCLUDE_PATH})

ADD_LIBRARY(mapreduce_nif MODULE ${COUCH_VIEW_MR_SRC})
SET_TARGET_PROPERTIES(mapreduce_nif PROPERTIES PREFIX "")

TARGET_LINK_LIBRARIES(mapreduce_nif ${COUCHBASE_UNRESOLVED}
                                    ${V8_LIBRARIES}
                                    ${COUCHBASE_MATH_LIBS})
SET_TARGET_PROPERTIES(mapreduce_nif PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/priv"
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/priv"
)
# mapreduce_nif is loaded into Erlang VM (beam.smp) which doesn't
# link the sanitizer libs and hence cannot successfully
# load mapreduce_nif if it has the sanitizers enabled. As such
# disable them.
remove_sanitizers(mapreduce_nif)

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
    # map from lib/couchdb/erlang/lib/mapreduce-1.0/priv/ to lib/
    SET_TARGET_PROPERTIES(mapreduce_nif PROPERTIES
                          INSTALL_RPATH "${ORIGIN}/../../../../..")
ENDIF ()


INSTALL(TARGETS mapreduce_nif
        DESTINATION ${MR_PRIV_PREFIX})
INSTALL(DIRECTORY ${COUCHDB_DEFAULT_LIB_DIR}/mapreduce/ebin
        DESTINATION ${MR_PREFIX})

ADD_SUBDIRECTORY(test)
ADD_SUBDIRECTORY(jsfunctions)
