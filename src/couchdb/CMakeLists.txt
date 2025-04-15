# Todo We should automagically build the module list..
CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/src/couch.app.src
               ${CMAKE_CURRENT_BINARY_DIR}/couch.app)

#todo remove this.. its currently just to diff the directories
CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/src/couch.app.src
               ${CMAKE_CURRENT_BINARY_DIR}/couch.app.tpl)

SET(COUCH_VERSION "1.2.0a-961ad59-git")
SET(COUCH_LIB_DIR ${CMAKE_ERL_LIB_INSTALL_PREFIX}/couch-${COUCH_VERSION})
ADD_SUBDIRECTORY(priv)

INSTALL(DIRECTORY ${COUCHDB_DEFAULT_LIB_DIR}/couch/ebin DESTINATION ${COUCH_LIB_DIR})
