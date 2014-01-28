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

couch_uprlibdir = $(localerlanglibdir)/couch_upr-1.0.0
couch_uprincludedir = $(couch_uprlibdir)/include
couch_uprebindir = $(couch_uprlibdir)/ebin

couch_uprinclude_DATA = $(include_files)
couch_uprebin_DATA = $(compiled_files)

include_files = \
    include/couch_upr.hrl

source_files = \
    src/couch_upr.app.src \
    src/couch_upr.erl \
    src/couch_upr_consumer.erl \
    src/couch_upr_fake_server.erl \
    src/couch_upr_producer.erl

test_files = \
    test/00-prepare.t \
    test/01-load.t \
    test/02-couch-upr.t

compiled_files = \
    ebin/couch_upr.app \
    ebin/couch_upr.beam \
    ebin/couch_upr_consumer.beam \
    ebin/couch_upr_fake_server.beam \
    ebin/couch_upr_producer.beam

EXTRA_DIST = $(include_files) $(source_files) $(test_files) test/run.tpl
CLEANFILES = $(compiled_files) test/run

noinst_SCRIPTS = test/run
TEST_JOBS ?= 3
maybe_jN = $(if $(shell prove --help | grep jobs), -j$(TEST_JOBS))

check:
	$(abs_top_builddir)/src/couch_upr/test/run $(abs_top_srcdir)/src/couch_upr/test/00-prepare.t
	$(abs_top_builddir)/src/couch_upr/test/run $(maybe_jN) $(abs_top_srcdir)/src/couch_upr/test

test/run: test/run.tpl
	sed -e "s|%abs_top_srcdir%|@abs_top_srcdir@|g" \
	    -e "s|%abs_top_builddir%|@abs_top_builddir@|g" > \
	$@ < $<
	chmod +x $@

ebin/%.app: src/%.app.src
	@mkdir -p ebin/
	sed -e "s|%version%|@version@|g" \
	< $< > $@

#ebin/%.beam: src/%.erl $(include_files) $(top_srcdir)/src/couchdb/couch_db.hrl $(top_srcdir)/src/couch_index_merger/include/couch_index_merger.hrl $(top_srcdir)/src/couch_index_merger/include/couch_view_merger.hrl
ebin/%.beam: src/%.erl $(include_files)
	@mkdir -p ebin/
	$(ERLC) -Wall -I$(top_srcdir)/src -I$(top_srcdir)/src/couchdb \
        -o ebin/ $(ERLC_FLAGS) ${TEST} $<;

#test/%.beam: test/%.erl $(include_files) $(top_srcdir)/src/couchdb/couch_db.hrl $(top_srcdir)/src/couch_index_merger/include/couch_index_merger.hrl $(top_srcdir)/src/couch_index_merger/include/couch_view_merger.hrl
test/%.beam: test/%.erl $(include_files)
	$(ERLC) -Wall -I$(top_srcdir)/src -I$(top_srcdir)/src/couchdb \
        -o test/ $(ERLC_FLAGS) ${TEST} $<;
