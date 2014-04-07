CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/default.ini.in
               ${CMAKE_CURRENT_BINARY_DIR}/default.ini)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/default_dev.ini.in
               ${CMAKE_CURRENT_BINARY_DIR}/default_dev.ini)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/local_dev.ini.in
               ${CMAKE_CURRENT_BINARY_DIR}/local_dev.ini)

INSTALL(FILES local.ini
              ${CMAKE_CURRENT_BINARY_DIR}/default.ini
        DESTINATION etc/couchdb)
