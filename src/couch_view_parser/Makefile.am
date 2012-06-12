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

COUCH_VIEW_PARSER_VERSION = 1.0
couch_view_parserebindir = $(localerlanglibdir)/couch_view_parser-$(COUCH_VIEW_PARSER_VERSION)/ebin
couch_view_parserprivdir = $(localerlanglibdir)/couch_view_parser-$(COUCH_VIEW_PARSER_VERSION)/priv

if WINDOWS
COUCH_VIEW_PARSER_OUTPUT_DIR = win32
couch_view_parser_msbuild = $(COUCH_VIEW_PARSER_OUTPUT_DIR)/msbuild.bat
couch_view_parser_vcproj = $(COUCH_VIEW_PARSER_OUTPUT_DIR)/couch_view_parser.vcxproj
WIN_ERL_INCLUDE = $(COUCH_VIEW_PARSER_OUTPUT_DIR)/win_erl_include.tmp
COUCH_VIEW_PARSER_NIF = $(COUCH_VIEW_PARSER_OUTPUT_DIR)/couch_view_parser_nif.dll
else
couch_view_parser_cxx_srcs = \
    couch_view_parser.cc \
    couch_view_parser_nif.cc \
    yajl/yajl.c \
    yajl/yajl_alloc.c \
    yajl/yajl_buf.c \
    yajl/yajl_encode.c \
    yajl/yajl_gen.c \
    yajl/yajl_lex.c \
    yajl/yajl_parser.c \
    yajl/yajl_tree.c \
    yajl/yajl_version.c

couch_view_parser_cxx_hdrs = \
    couch_view_parser.h \
    erl_nif_compat.h \
    yajl/yajl_alloc.h \
    yajl/yajl_buf.h \
    yajl/yajl_bytestack.h \
    yajl/yajl_encode.h \
    yajl/yajl_lex.h \
    yajl/yajl_parser.h \
    yajl/yajl_common.h \
    yajl/yajl_gen.h \
    yajl/yajl_parse.h \
    yajl/yajl_tree.h \
    yajl/yajl_version.h
endif
couch_view_parser_file_collection = \
    couch_view_parser.app.in \
    couch_view_parser.erl

couch_view_parserebin_make_generated_file_list = \
    couch_view_parser.app \
    couch_view_parser.beam

test_files = \
    test/01-map-view.t \
    test/02-reduce-view.t

EXTRA_DIST = \
    $(couch_view_parser_cxx_hdrs) \
    $(couch_view_parser_file_collection) \
    $(test_files) \
    test/run.tpl

CLEANFILES = \
    $(couch_view_parserebin_make_generated_file_list) \
    priv/couch_view_parser_nif.so

noinst_SCRIPTS = test/run

if WINDOWS
couch_view_parserebin_DATA = $(COUCH_VIEW_PARSER_NIF) \
    $(couch_view_parserebin_make_generated_file_list)
else
couch_view_parserebin_DATA = \
    $(couch_view_parserebin_make_generated_file_list)
endif

if !WINDOWS
couch_view_parserpriv_LTLIBRARIES = couch_view_parser_nif.la
couch_view_parser_nif_la_AM_CPPFLAGS = $(AM_CPPFLAGS) -Icouch_view_parser-$(COUCH_VIEW_PARSER_VERSION) -Iyajl
couch_view_parser_nif_la_CPPFLAGS = -std=c99 -Wall
couch_view_parser_nif_la_SOURCES = $(couch_view_parser_cxx_srcs)
couch_view_parser_nif_la_LDFLAGS = -module -avoid-version

priv/couch_view_parser_nif.so: couch_view_parser_nif.la
	@mkdir -p ./priv
	cp .libs/couch_view_parser_nif.so $@

all: priv/couch_view_parser_nif.so
endif

if WINDOWS
$(COUCH_VIEW_PARSER_NIF) : $(couch_view_parser_msbuild) $(couch_view_parser_vcproj)
	$(MKDIR_P) "$(couch_view_parserprivdir)" || true
	(cd win32 && cmd /c msbuild.bat)
	cp $(COUCH_VIEW_PARSER_NIF) $(couch_view_parserprivdir)

all: $(COUCH_VIEW_PARSER_NIF)

$(couch_view_parser_msbuild): $(couch_view_parser_msbuild).tpl
	sed -e "s|%msbuild_dir%|$(msbuild_dir)|" \
	    -e "s|%msbuild_name%|$(msbuild_name)|" \
	    -e "s|^/cygdrive/\([a-zA-Z]\)|\1:|" \
	< $< > $@

$(couch_view_parser_vcproj): $(couch_view_parser_vcproj).tpl $(WIN_ERL_INCLUDE)
	 sed -e "s|%ERLANG_INCLUDE%|`cat $(WIN_ERL_INCLUDE)`|" \
	     -e "s|%COUCH_VIEW_PARSER_VERSION%|$(COUCH_VIEW_PARSER_VERSION)|" \
	< $< > $@

$(WIN_ERL_INCLUDE):$(ERLANG_INCLUDE)
	echo $< | sed -e "s|^/cygdrive/\([a-zA-Z]\)|\1:|" > $@

endif

check:
	$(abs_top_builddir)/src/couch_view_parser/test/run $(abs_top_srcdir)/src/couch_view_parser/test

test/run: test/run.tpl
	sed -e "s|%abs_top_srcdir%|@abs_top_srcdir@|g" \
	    -e "s|%abs_top_builddir%|@abs_top_builddir@|g" > \
	$@ < $<
	chmod +x $@

%.app: %.app.in
	cp $< $@

%.beam: %.erl
	$(ERLC) $(ERLC_FLAGS) $<
