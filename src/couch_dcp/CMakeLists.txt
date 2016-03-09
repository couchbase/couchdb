SET(SOURCES
  src/cb_auth_info.erl
  src/couch_dcp_client.erl
  src/couch_dcp_consumer.erl
  src/couch_dcp_fake_server.erl
  src/couch_dcp_producer.erl)
SET(HEADERS
  include/couch_dcp.hrl
  include/couch_dcp_typespecs.hrl)

ADD_ERLANG_INCLUDE_DIR(${CMAKE_CURRENT_SOURCE_DIR}/../couchdb)
ADD_ERLANG_INCLUDE_DIR(${CMAKE_CURRENT_SOURCE_DIR}/..)

ERL_BUILD_OTP (APPNAME "couch_dcp" VERSION 1.0.0
  INSTALL_PATH ${CMAKE_ERL_LIB_INSTALL_PREFIX}
  HEADERS ${HEADERS} SOURCES ${SOURCES} DEPENDS ${COUCHDB_ALL_ERLANG_HEADERS})

ADD_SUBDIRECTORY(test)
