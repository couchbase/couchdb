SET(COUCH_AUDIT_VERSION "1.0.0")
SET(COUCH_AUDIT_PREFIX ${CMAKE_ERL_LIB_INSTALL_PREFIX}/couch_audit-${COUCH_AUDIT_VERSION})

INSTALL(DIRECTORY ${COUCHDB_DEFAULT_LIB_DIR}/couch_audit/ebin
        DESTINATION ${COUCH_AUDIT_PREFIX})
