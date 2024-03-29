SET(COUCH_PRIV_PREFIX ${COUCH_LIB_DIR}/priv)
SET(COUCH_PRIV_LIB_PREFIX ${COUCH_PRIV_PREFIX}/lib)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/stat_descriptions.cfg.in
               ${CMAKE_CURRENT_BINARY_DIR}/stat_descriptions.cfg)

INCLUDE_DIRECTORIES(BEFORE ${ICU_INCLUDE_DIR}
                           ${Platform_SOURCE_DIR}/include
                           ${ERLANG_INCLUDE_PATH})

ADD_DEFINITIONS(-DXP_UNIX)

SET(COUCH_EJSON_COMPARE_SRC couch_ejson_compare/couch_ejson_compare.c
                            couch_ejson_compare/couch_raw_json_compare.c
                            couch_ejson_compare/couch_ejson_compare_utils.c)

ADD_LIBRARY(couch_ejson_compare MODULE ${COUCH_EJSON_COMPARE_SRC})
SET_TARGET_PROPERTIES(couch_ejson_compare PROPERTIES PREFIX "")
TARGET_LINK_LIBRARIES(couch_ejson_compare
  ${COUCHBASE_UNRESOLVED}
  ${ICU_LIBRARIES}
  platform_cbassert_unsanitized)

# couch_ejson_compare is loaded into Erlang VM (beam.smp) which
# doesn't link the sanitizer libs and hence cannot successfully load
# couch_ejson_compare if it has the sanitizers enabled. As such
# disable them.
remove_sanitizers(couch_ejson_compare)

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
# map from lib/couchdb/erlang/lib/couch-1.2.0a-961ad59-git/priv/lib to lib/
SET_TARGET_PROPERTIES(couch_ejson_compare PROPERTIES
                      INSTALL_RPATH "${ORIGIN}/../../../../../..")
ENDIF ()

INSTALL(TARGETS couch_ejson_compare
        DESTINATION ${COUCH_PRIV_LIB_PREFIX})

INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/stat_descriptions.cfg
        DESTINATION ${COUCH_PRIV_PREFIX})
