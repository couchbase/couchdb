SET(SOURCES
  src/couch_index_barrier.erl
  src/couch_set_view_compactor.erl
  src/couch_set_view_ddoc_cache.erl
  src/couch_set_view_dev.erl
  src/couch_set_view.erl
  src/couch_set_view_group.erl
  src/couch_set_view_http.erl
  src/couch_set_view_mapreduce.erl
  src/couch_set_view_updater.erl
  src/couch_set_view_updater_helper.erl
  src/couch_set_view_util.erl
  src/mapreduce_view.erl)
SET(HEADERS
  include/couch_set_view.hrl)

ADD_ERLANG_INCLUDE_DIR(${CMAKE_CURRENT_SOURCE_DIR}/../couchdb)
ADD_ERLANG_INCLUDE_DIR(${CMAKE_CURRENT_SOURCE_DIR}/..)

ERL_BUILD_OTP (APPNAME "couch_set_view" VERSION 1.0.0
  INSTALL_PATH ${CMAKE_ERL_LIB_INSTALL_PREFIX}
  HEADERS ${HEADERS} SOURCES ${SOURCES} DEPENDS ${COUCHDB_ALL_ERLANG_HEADERS})

ADD_SUBDIRECTORY(test)
