FILE(GLOB SRC src/*.erl)
ADD_ERLANG_INCLUDE_DIR(${CMAKE_CURRENT_SOURCE_DIR}/../couchdb)
ADD_ERLANG_INCLUDE_DIR(${CMAKE_CURRENT_SOURCE_DIR}/..)
ADD_ERLANG_INCLUDE_DIR(${CMAKE_CURRENT_SOURCE_DIR}/include)


CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/src/couch_set_view.app.src
               ${CMAKE_CURRENT_BINARY_DIR}/ebin/couch_set_view.app)


ERL_BUILD_OTP("couch_set_view" ${SRC})
SET(COUCH_SET_VIEW_PREFIX ${CMAKE_ERL_LIB_INSTALL_PREFIX}/couch_set_view-1.0.0)

SET(COUCH_SET_VIEW_INCLUDE_FILES include/couch_set_view.hrl
                                 src/couch_set_view_updater.hrl)

INSTALL(FILES ${COUCH_SET_VIEW_INCLUDE_FILES}
        DESTINATION ${COUCH_SET_VIEW_PREFIX}/include)

INSTALL(FILES ${outfiles}
              ${CMAKE_CURRENT_BINARY_DIR}/ebin/couch_set_view.app
        DESTINATION ${COUCH_SET_VIEW_PREFIX}/ebin)

ADD_SUBDIRECTORY(test)
