CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/couchdb.tpl.in
               ${CMAKE_CURRENT_BINARY_DIR}/couchdb)

INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/couchdb
        DESTINATION etc/logrotate.d)
