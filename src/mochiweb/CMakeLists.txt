SET(SOURCES
  mochifmt.erl
  mochifmt_records.erl
  mochifmt_std.erl
  mochiglobal.erl
  mochihex.erl
  mochijson2.erl
  mochijson.erl
  mochilists.erl
  mochilogfile2.erl
  mochinum.erl
  mochitemp.erl
  mochiutf8.erl
  mochiweb_acceptor.erl
  mochiweb_app.erl
  mochiweb_base64url.erl
  mochiweb_charref.erl
  mochiweb_clock.erl
  mochiweb_cookies.erl
  mochiweb_cover.erl
  mochiweb_echo.erl
  mochiweb.erl
  mochiweb_headers.erl
  mochiweb_html.erl
  mochiweb_http.erl
  mochiweb_io.erl
  mochiweb_mime.erl
  mochiweb_multipart.erl
  mochiweb_request.erl
  mochiweb_request_tests.erl
  mochiweb_response.erl
  mochiweb_session.erl
  mochiweb_skel.erl
  mochiweb_socket.erl
  mochiweb_socket_server.erl
  mochiweb_sup.erl
  mochiweb_util.erl
  mochiweb_websocket.erl
  reloader.erl)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/mochiweb.app.src
               ${CMAKE_CURRENT_BINARY_DIR}/mochiweb.app)

ERL_BUILD(APPNAME "mochiweb" SOURCES ${SOURCES} DEPENDS internal.hrl)
INSTALL(FILES ${outfiles}
               ${CMAKE_CURRENT_BINARY_DIR}/mochiweb.app
        DESTINATION ${CMAKE_ERL_LIB_INSTALL_PREFIX}/mochiweb-1.4.1/ebin)
