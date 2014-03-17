IF (WIN32)
   CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/couchdb.bat.tpl.in
                  ${CMAKE_CURRENT_BINARY_DIR}/couchdb.bat.tpl)

   CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/couchdb.bat.tpl.in
                  ${CMAKE_CURRENT_BINARY_DIR}/couchdb.bat)

   INSTALL(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/couchdb.bat
           DESTINATION bin)

ELSE (WIN32)
   CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/couchjs.tpl.in
                  ${CMAKE_CURRENT_BINARY_DIR}/couchjs.tpl)

   CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/couchjs.tpl.in
                  ${CMAKE_CURRENT_BINARY_DIR}/couchjs)

   CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/couchjs.tpl.in
                  ${CMAKE_CURRENT_BINARY_DIR}/couchjs_dev)

   CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/couchdb.tpl.in
                  ${CMAKE_CURRENT_BINARY_DIR}/couchdb.tpl)

   CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/couchdb.tpl.in
                  ${CMAKE_CURRENT_BINARY_DIR}/couchdb)

   INSTALL(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/couchjs
                    ${CMAKE_CURRENT_BINARY_DIR}/couchdb
           DESTINATION bin)
ENDIF (WIN32)
