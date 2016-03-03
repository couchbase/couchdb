ERL_BUILD("etap" etap.erl)
INSTALL(FILES ${outfiles}
    DESTINATION ${CMAKE_ERL_LIB_INSTALL_PREFIX}/etap/ebin)
