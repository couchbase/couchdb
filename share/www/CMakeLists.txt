
INSTALL(FILES _sidebar.html
              config.html
              couch_tests.html
              custom_test.html
              database.html
              document.html
              favicon.ico
              index.html
              replicator.html
              session.html
              status.html
              verify_install.html
        DESTINATION share/couchdb/www)

INSTALL(FILES dialog/_admin_party.html
              dialog/_change_password.html
              dialog/_compact_cleanup.html
              dialog/_create_admin.html
              dialog/_create_config.html
              dialog/_create_database.html
              dialog/_database_security.html
              dialog/_delete_database.html
              dialog/_delete_document.html
              dialog/_login.html
              dialog/_save_view_as.html
              dialog/_share_test_reports.html
              dialog/_signup.html
              dialog/_upload_attachment.html
        DESTINATION share/couchdb/www/dialog)

INSTALL(FILES image/add.png
              image/apply.gif
              image/bg.png
              image/cancel.gif
              image/compact.png
              image/delete-mini.png
              image/delete.png
              image/grippie.gif
              image/hgrad.gif
              image/key.png
              image/load.png
              image/logo.png
              image/order-asc.gif
              image/order-desc.gif
              image/path.gif
              image/progress.gif
              image/rarrow.png
              image/run-mini.png
              image/run.png
              image/running.png
              image/save.png
              image/sidebar-toggle.png
              image/spinner.gif
              image/spinner_33.gif
              image/spinner_6b.gif
              image/test_failure.gif
              image/test_success.gif
              image/thead-key.gif
              image/thead.gif
              image/toggle-collapse.gif
              image/toggle-expand.gif
              image/twisty.gif
        DESTINATION share/couchdb/www/image)

INSTALL(FILES script/base64.js
              script/couch.js
              script/couch_test_runner.js
              script/couch_tests.js
              script/futon.browse.js
              script/futon.format.js
              script/futon.js
              script/jquery-ui-1.8.11.custom.min.js
              script/jquery.couch.js
              script/jquery.dialog.js
              script/jquery.editinline.js
              script/jquery.form.js
              script/jquery.js
              script/jquery.resizer.js
              script/jquery.suggest.js
              script/json2.js
	      script/oauth.js
              script/sha1.js
        DESTINATION share/couchdb/www/script)


INSTALL(FILES script/jspec/jspec.css
              script/jspec/jspec.jquery.js
              script/jspec/jspec.js
              script/jspec/jspec.xhr.js
        DESTINATION share/couchdb/www/script/jspec)

INSTALL(FILES script/test/all_docs.js
              script/test/attachment_names.js
              script/test/attachment_paths.js
              script/test/attachment_ranges.js
              script/test/attachment_views.js
              script/test/attachments.js
              script/test/attachments_multipart.js
              script/test/auth_cache.js
              script/test/basics.js
              script/test/batch_save.js
              script/test/bulk_docs.js
              script/test/changes.js
              script/test/compact.js
              script/test/config.js
              script/test/conflicts.js
              script/test/content_negotiation.js
              script/test/cookie_auth.js
              script/test/copy_doc.js
              script/test/delayed_commits.js
              script/test/design_docs.js
              script/test/design_options.js
              script/test/design_paths.js
              script/test/erlang_views.js
              script/test/etags_head.js
              script/test/etags_views.js
              script/test/form_submit.js
              script/test/http.js
              script/test/invalid_docids.js
              script/test/jsonp.js
              script/test/large_docs.js
              script/test/list_views.js
              script/test/lorem.txt
              script/test/lorem_b64.txt
              script/test/lots_of_docs.js
              script/test/method_override.js
              script/test/multiple_rows.js
              script/test/oauth.js
              script/test/proxyauth.js
              script/test/purge.js
              script/test/reader_acl.js
              script/test/recreate_doc.js
              script/test/reduce.js
              script/test/reduce_builtin.js
              script/test/reduce_false.js
              script/test/reduce_false_temp.js
              script/test/replication.js
              script/test/replicator_db.js
              script/test/rev_stemming.js
              script/test/rewrite.js
              script/test/security_validation.js
              script/test/show_documents.js
              script/test/stats.js
              script/test/update_documents.js
              script/test/users_db.js
              script/test/utf8.js
              script/test/uuids.js
              script/test/view_collation.js
              script/test/view_collation_raw.js
              script/test/view_compaction.js
              script/test/view_conflicts.js
              script/test/view_errors.js
              script/test/view_include_docs.js
              script/test/view_merging.js
              script/test/view_multi_key_all_docs.js
              script/test/view_multi_key_design.js
              script/test/view_multi_key_temp.js
              script/test/view_offsets.js
              script/test/view_pagination.js
              script/test/view_sandboxing.js
              script/test/view_update_seq.js
              script/test/view_xml.js
        DESTINATION share/couchdb/www/script/test)

INSTALL(FILES spec/couch_js_class_methods_spec.js
              spec/couch_js_instance_methods_1_spec.js
              spec/couch_js_instance_methods_2_spec.js
              spec/couch_js_instance_methods_3_spec.js
              spec/custom_helpers.js
              spec/jquery_couch_js_class_methods_spec.js
              spec/jquery_couch_js_instance_methods_1_spec.js
              spec/jquery_couch_js_instance_methods_2_spec.js
              spec/jquery_couch_js_instance_methods_3_spec.js
              spec/run.html
        DESTINATION share/couchdb/www/spec)

INSTALL(FILES style/jquery-ui-1.8.11.custom.css
              style/layout.css
        DESTINATION share/couchdb/www/style)
