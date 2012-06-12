## Licensed under the Apache License, Version 2.0 (the "License"); you may not
## use this file except in compliance with the License. You may obtain a copy of
## the License at
##
##   http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
## WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
## License for the specific language governing permissions and limitations under
## the License.

couch_index_mergerlibdir = $(localerlanglibdir)/couch_index_merger-1.0.0
couch_index_mergerincludedir = $(couch_index_mergerlibdir)/include
couch_index_mergerebindir = $(couch_index_mergerlibdir)/ebin

couch_index_mergerinclude_DATA = $(include_files)
couch_index_mergerebin_DATA = $(compiled_files)

include_files = \
    include/couch_index_merger.hrl \
    include/couch_view_merger.hrl

source_files = \
    src/couch_index_merger.app.src \
    src/couch_index_merger.erl \
    src/couch_view_merger.erl \
    src/couch_view_merger_queue.erl \
    src/couch_http_view_streamer.erl \
    src/couch_httpd_view_merger.erl \
    src/couch_skew.erl

compiled_files = \
    ebin/couch_index_merger.app \
    ebin/couch_index_merger.beam \
    ebin/couch_view_merger.beam \
    ebin/couch_view_merger_queue.beam \
    ebin/couch_http_view_streamer.beam \
    ebin/couch_httpd_view_merger.beam \
    ebin/couch_skew.beam

EXTRA_DIST = $(include_files) $(source_files)
CLEANFILES = $(compiled_files)

ebin/%.app: src/%.app.src
	@mkdir -p ebin/
	sed -e "s|%version%|@version@|g" \
	< $< > $@

ebin/%.beam: src/%.erl $(include_files) $(top_srcdir)/src/couchdb/couch_db.hrl $(top_srcdir)/src/couch_set_view/include/couch_set_view.hrl
	@mkdir -p ebin/
	$(ERLC) -Wall -I$(top_srcdir)/src -I$(top_srcdir)/src/couchdb \
        -o ebin/ $(ERLC_FLAGS) ${TEST} $<;
