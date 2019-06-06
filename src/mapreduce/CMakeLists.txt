SET(MR_VERSION "1.0")
SET(MR_PREFIX ${CMAKE_ERL_LIB_INSTALL_PREFIX}/mapreduce-${MR_VERSION})
SET(MR_EBIN_PREFIX ${MR_PREFIX}/ebin)
SET(MR_PRIV_PREFIX ${MR_PREFIX}/priv)


CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/test/run.tpl
               ${CMAKE_CURRENT_BINARY_DIR}/test/run)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/mapreduce.app.in
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

ERL_BUILD(APPNAME "mapreduce" SOURCES mapreduce.erl)

INSTALL(TARGETS mapreduce_nif
        DESTINATION ${MR_PRIV_PREFIX})
INSTALL(FILES ${outfiles}
               ${CMAKE_CURRENT_BINARY_DIR}/mapreduce.app
        DESTINATION ${MR_EBIN_PREFIX})

# icudtl.dat file is loaded in install/bin in Windows and install/lib
# in LINUX, but the mapreduce files requires it to be present in
# ${MR_PRIV_PREFIX} directory and other dependencies require them to be
# present in install/bin directory, so we copy it from lib to bin
# directory and lib to ${MR_PRIV_PREFIX} on LINUX, in Windows, we
# just copy it to ${MR_PRIV_PREFIX} from bin.
IF (WIN32)
    INSTALL(FILES ${CMAKE_INSTALL_PREFIX}/bin/icudtl.dat
            DESTINATION ${MR_PRIV_PREFIX})
ELSE ()
    INSTALL(FILES ${CMAKE_INSTALL_PREFIX}/lib/icudtl.dat
            DESTINATION ${MR_PRIV_PREFIX})
    INSTALL(FILES ${CMAKE_INSTALL_PREFIX}/lib/icudtl.dat
            DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)
    INSTALL(FILES ${CMAKE_INSTALL_PREFIX}/lib/icudtl.dat
            DESTINATION ${CMAKE_BINARY_DIR}/couchdb/src/mapreduce/priv)
ENDIF ()

ADD_SUBDIRECTORY(test)
ADD_SUBDIRECTORY(jsfunctions)
