SET(SOURCES
  src/couch_httpd_view_merger.erl
  src/couch_http_view_streamer.erl
  src/couch_index_merger.erl
  src/couch_index_merger_validation.erl
  src/couch_skew.erl
  src/couch_view_merger.erl
  src/couch_view_merger_queue.erl
  src/couch_index_sup.erl
  src/couch_query_logger.erl
  src/couch_index_app.erl)
SET(HEADERS
  include/couch_index_merger.hrl
  include/couch_view_merger.hrl)

ADD_ERLANG_INCLUDE_DIR(${CMAKE_CURRENT_SOURCE_DIR}/../../src)
ADD_ERLANG_INCLUDE_DIR(${CMAKE_CURRENT_SOURCE_DIR}/../../src/couchdb)

ERL_BUILD_OTP (APPNAME "couch_index_merger" VERSION 1.0.0
  INSTALL_PATH ${CMAKE_ERL_LIB_INSTALL_PREFIX}
  HEADERS ${HEADERS} SOURCES ${SOURCES} DEPENDS ${COUCHDB_ALL_ERLANG_HEADERS})
