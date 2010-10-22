// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.



// Used by replication test
if (typeof window == 'undefined' || !window) {
  CouchDB.host = "127.0.0.1:5984";
  CouchDB.inBrowser = false;
} else {
  CouchDB.host = window.location.host;
  CouchDB.inBrowser = true;
}

CouchDB.urlPrefix = "..";
var couchTests = {};
var allCouchTests = [ 
    "basics.js"
  , "all_docs.js"
  , "attachments.js"
  , "attachments_multipart.js"
  , "attachment_names.js"
  , "attachment_paths.js"
  , "attachment_views.js"
  , "auth_cache.js"
  , "batch_save.js"
  , "bulk_docs.js"
  , "changes.js"
  , "compact.js"
  , "config.js"
  , "conflicts.js"
  , "content_negotiation.js"
  , "cookie_auth.js"
  , "copy_doc.js"
  , "delayed_commits.js"
  , "design_docs.js"
  , "design_options.js"
  , "design_paths.js"
  , "erlang_views.js"
  , "etags_head.js"
  , "etags_views.js"
  , "form_submit.js"
  , "http.js"
  , "invalid_docids.js"
  , "jsonp.js"
  , "large_docs.js"
  , "list_views.js"
  , "lots_of_docs.js"
  , "method_override.js"
  , "multiple_rows.js"
  , "oauth.js"
  , "proxyauth.js"
  , "purge.js"
  , "reader_acl.js"
  , "recreate_doc.js"
  , "reduce.js"
  , "reduce_builtin.js"
  , "reduce_false.js"
  , "reduce_false_temp.js"
  , "replication.js"
  , "replicator_db.js"
  , "rev_stemming.js"
  , "rewrite.js"
  , "security_validation.js"
  , "show_documents.js"
  , "stats.js"
  , "update_documents.js"
  , "users_db.js"
  , "utf8.js"
  , "uuids.js"
  , "view_collation.js"
  , "view_collation_raw.js"
  , "view_conflicts.js"
  , "view_compaction.js"
  , "view_errors.js"
  , "view_include_docs.js"
  , "view_multi_key_all_docs.js"
  , "view_multi_key_design.js"
  , "view_multi_key_temp.js"
  , "view_offsets.js"
  , "view_pagination.js"
  , "view_sandboxing.js"
  , "view_update_seq.js"
  , "view_xml.js"
]

function loadTests (tests, callback) {
  // Load a list of tests
  
  var loadScript = function (url, callback){
    // Load a single script, fire callback when finished.
    var script = document.createElement("script")
    script.type = "text/javascript";

    if (script.readyState){  //IE
        script.onreadystatechange = function(){
            if (script.readyState == "loaded" ||
                    script.readyState == "complete"){
                script.onreadystatechange = null;
                callback();
            }
        };
    } else {  //Others
        script.onload = function(){
            callback();
        };
    }
    
    script.src = url;
    document.body.appendChild(script);
  }
  
  var l = tests.length
    , i = 0
    , failed = []
    ;
  
  tests.forEach(function (test) {
    loadScript('script/test/'+test, function () {
      i += 1;
      if (i === l) callback()
    });   
  })
}
