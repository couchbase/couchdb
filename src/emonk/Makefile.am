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

emonklibdir = $(localerlanglibdir)/emonk-$(version)
emonkebindir = $(emonklibdir)/ebin
emonkprivdir = $(emonklibdir)/priv

emonkebin_DATA = $(beam_files) $(app_files)
CLEANFILES = $(beam_files)
EXTRA_DIST = $(erl_files) $(app_files)

emonkpriv_LTLIBRARIES = emonk.la
emonk_la_SOURCES = $(c_files)
emonk_la_LDFLAGS = -module -avoid-version
emonk_la_LIBADD = @JSLIB@ -lnspr4

priv/emonk.so: emonk.la
	@mkdir -p priv
	@rm -f $@
	$(LN_S) $(abs_top_builddir)/src/emonk/.libs/emonk.so $@

all: priv/emonk.so

erl_files = \
	src/emonk.erl

beam_files = \
	ebin/emonk.beam

app_files = \
	ebin/emonk.app

c_files = \
	c_src/main.c \
	c_src/queue.c \
	c_src/to_erl.c \
	c_src/to_js.c \
	c_src/util.c \
	c_src/vm.c

ebin/%.beam: src/%.erl
	$(ERLC) $(ERLC_FLAGS) -o `dirname $@` $<


