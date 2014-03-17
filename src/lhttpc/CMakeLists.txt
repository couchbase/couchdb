FILE(GLOB LHTTP_SRC *.erl)
CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/lhttpc.app.src
               ${CMAKE_CURRENT_BINARY_DIR}/lhttpc.app)

ERL_BUILD("lhttpc" ${LHTTP_SRC})
INSTALL(FILES ${outfiles}
              ${CMAKE_CURRENT_BINARY_DIR}/lhttpc.app
        DESTINATION ${CMAKE_ERL_LIB_INSTALL_PREFIX}/lhttpc-1.3/ebin)
