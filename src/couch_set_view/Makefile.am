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

couch_set_viewlibdir = $(localerlanglibdir)/couch_set_view-1.0.0
couch_set_viewincludedir = $(couch_set_viewlibdir)/include
couch_set_viewebindir = $(couch_set_viewlibdir)/ebin

couch_set_viewinclude_DATA = $(include_files)
couch_set_viewebin_DATA = $(compiled_files)

include_files = \
    include/couch_set_view.hrl

source_files = \
    src/couch_set_view.app.src \
    src/couch_set_view.erl \
    src/couch_set_view_http.erl \
    src/couch_set_view_group.erl \
    src/couch_set_view_updater.erl \
    src/couch_set_view_compactor.erl \
    src/couch_set_view_util.erl \
    src/couch_db_set.erl

test_files = \
    test/01-load.t

compiled_files = \
    ebin/couch_set_view.app \
    ebin/couch_set_view.beam \
    ebin/couch_set_view_http.beam \
    ebin/couch_set_view_group.beam \
    ebin/couch_set_view_updater.beam \
    ebin/couch_set_view_compactor.beam \
    ebin/couch_set_view_util.beam \
    ebin/couch_db_set.beam

EXTRA_DIST = $(include_files) $(source_files) $(test_files)
CLEANFILES = $(compiled_files)

check:
	$(abs_top_builddir)/test/etap/run $(abs_top_srcdir)/src/couch_set_view/test

ebin/%.app: src/%.app.src
	@mkdir -p ebin/
	sed -e "s|%version%|@version@|g" \
	< $< > $@

ebin/%.beam: src/%.erl $(include_files)
	@mkdir -p ebin/
	$(ERLC) -Wall -I$(top_srcdir)/src -I$(top_srcdir)/src/couchdb \
        -o ebin/ $(ERLC_FLAGS) ${TEST} $<;

