IF (APPLE)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/org.apache.couchdb.plist.tpl.in
               ${CMAKE_CURRENT_BINARY_DIR}/org.apache.couchdb.plist)


INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/org.apache.couchdb.plist
        DESTINATION Library/LaunchDaemons )

ENDIF(APPLE)
