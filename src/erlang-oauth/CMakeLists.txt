# Removed oauth_rsa_sha1.beam until we require R12B5 or
# we add a ./configure option to enable it.
#FILE(GLOB OAUTH_SRC *.erl)

SET(OAUTH_SRC oauth.erl oauth_http.erl oauth_uri.erl
              oauth_hmac_sha1.erl oauth_plaintext.erl oauth_unix.erl)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/oauth.app.in
               ${CMAKE_CURRENT_BINARY_DIR}/oauth.app)

ERL_BUILD("erlang_oauth" ${OAUTH_SRC})
INSTALL(FILES ${outfiles}
              ${CMAKE_CURRENT_BINARY_DIR}/oauth.app
        DESTINATION ${CMAKE_ERL_LIB_INSTALL_PREFIX}/erlang-oauth/ebin)
