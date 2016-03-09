SET(SOURCES
  lhttpc_client.erl
  lhttpc.erl
  lhttpc_lib.erl
  lhttpc_manager.erl
  lhttpc_sock.erl
  lhttpc_sup.erl)
SET(HEADERS
  lhttpc.hrl
  lhttpc_types.hrl)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/lhttpc.app.src
               ${CMAKE_CURRENT_BINARY_DIR}/lhttpc.app)

ERL_BUILD(APPNAME "lhttpc" SOURCES ${SOURCES} DEPENDS ${HEADERS})
INSTALL(FILES ${outfiles}
              ${CMAKE_CURRENT_BINARY_DIR}/lhttpc.app
        DESTINATION ${CMAKE_ERL_LIB_INSTALL_PREFIX}/lhttpc-1.3/ebin)
