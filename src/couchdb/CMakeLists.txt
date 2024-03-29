SET(COUCHDB_SRC file2.erl
            couch_app.erl
            file_sorter_2.erl
            couch_btree.erl
            couch_btree_copy.erl
            couch_changes.erl
            couch_compaction_daemon.erl
            couch_compress.erl
            couch_config.erl
            couch_config_writer.erl
            couch_db.erl
            couch_db_update_notifier.erl
            couch_db_update_notifier_sup.erl
            couch_doc.erl
            couch_ejson_compare.erl
            couch_event_sup.erl
            couch_system_event.erl
            couch_file.erl
            couch_file_write_guard.erl
            couch_db_frontend.erl
            couch_httpd.erl
            couch_httpd_db.erl
            couch_httpd_external.erl
            couch_httpd_view.erl
            couch_httpd_misc_handlers.erl
            couch_log.erl
            couch_os_process.erl
            couch_primary_sup.erl
            couch_query_servers.erl
            couch_ref_counter.erl
            couch_rep_sup.erl
            couch_replication_manager.erl
            couch_replication_notifier.erl
            couch_replicator.erl
            couch_replicator_worker.erl
            couch_replicator_utils.erl
            couch_secondary_sup.erl
            couch_server.erl
            couch_server_sup.erl
            couch_task_status.erl
            couch_util.erl
            couch_uuids.erl
            couch_view.erl
            couch_view_compactor.erl
            couch_view_updater.erl
            couch_view_group.erl
            couch_view_mapreduce.erl
            couch_db_updater.erl
            couch_work_queue.erl
            json_stream_parse.erl)

# Todo We should automagically build the module list..
CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/couch.app.tpl.in
               ${CMAKE_CURRENT_BINARY_DIR}/couch.app)

#todo remove this.. its currently just to diff the directories
CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/couch.app.tpl.in
               ${CMAKE_CURRENT_BINARY_DIR}/couch.app.tpl)


ERL_BUILD(APPNAME "couchdb" SOURCES ${COUCHDB_SRC}
          DEPENDS ${COUCHDB_ALL_ERLANG_HEADERS})
SET(COUCH_LIB_DIR ${CMAKE_ERL_LIB_INSTALL_PREFIX}/couch-1.2.0a-961ad59-git)
SET(COUCH_LIB_INCLUDE_DIR ${COUCH_LIB_DIR}/include)
SET(COUCH_LIB_BIN_DIR ${COUCH_LIB_DIR}/ebin)
ADD_SUBDIRECTORY(priv)

INSTALL(FILES ${outfiles}
              ${CMAKE_CURRENT_BINARY_DIR}/couch.app
        DESTINATION ${COUCH_LIB_BIN_DIR})

INSTALL(FILES couch_api_wrap.hrl
              couch_db.hrl
              couch_js_functions.hrl
              couch_replicator.hrl
        DESTINATION ${COUCH_LIB_DIR}/include)
