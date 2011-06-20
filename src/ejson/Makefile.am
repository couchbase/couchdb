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

ejsonebindir = $(localerlanglibdir)/ejson-0.1.0/ebin
ejsonprivdir = $(localerlanglibdir)/ejson-0.1.0/priv

if WINDOWS
EJSON_OUTPUT_DIR = win32
ejson_msbuild = $(EJSON_OUTPUT_DIR)/msbuild.bat
ejson_vcproj = $(EJSON_OUTPUT_DIR)/ejson.vcxproj
WIN_ERL_INCLUDE = $(EJSON_OUTPUT_DIR)/win_erl_include.tmp
EJSON = $(EJSON_OUTPUT_DIR)/ejson.dll
endif

CLEANFILES = \
    $(ejsonebin_make_generated_file_list) \
    $(ejsonpriv_make_generated_file_list) \
    priv/ejson.so

EJSON_C_SRCS = \
	ejson.c \
	decode.c \
	encode.c \
	yajl/yajl_alloc.c \
	yajl/yajl_buf.c \
	yajl/yajl.c \
	yajl/yajl_encode.c \
	yajl/yajl_gen.c \
	yajl/yajl_lex.c \
	yajl/yajl_parser.c

EJSON_C_HDRS = \
    erl_nif_compat.h \
    yajl/yajl_alloc.h \
    yajl/yajl_buf.h \
    yajl/yajl_bytestack.h \
    yajl/yajl_common.h \
    yajl/yajl_encode.h \
    yajl/yajl_gen.h \
    yajl/yajl_lex.h \
    yajl/yajl_parse.h \
    yajl/yajl_parser.h

ejson_file_collection = \
    ejson.app.in \
    ejson.erl \
    mochijson2.erl \
    mochinum.erl

ejsonebin_make_generated_file_list = \
    ejson.app \
    ejson.beam \
    mochijson2.beam \
    mochinum.beam

EXTRA_DIST = \
	$(EJSON_C_HDRS) \
	$(ejson_file_collection)

ejsonebin_DATA = \
    $(ejsonebin_make_generated_file_list)

if !WINDOWS
if USE_OTP_NIFS
ejsonpriv_LTLIBRARIES = ejson.la

ejson_la_SOURCES = $(EJSON_C_SRCS)
ejson_la_LDFLAGS = -module -avoid-version

priv/ejson.so: ejson.la
	@mkdir -p ./priv
	cp .libs/ejson.so $@

all: priv/ejson.so
endif
endif

if WINDOWS
$(EJSON) : $(ejson_msbuild) $(ejson_vcproj)
	$(MKDIR_P) "$(ejsonprivdir)" || true
	(cd win32 && ./msbuild.bat)
	cp $(EJSON) $(ejsonprivdir)

all: $(EJSON)

$(ejson_msbuild): $(ejson_msbuild).tpl
	sed -e "s|%msbuild_dir%|$(msbuild_dir)|" \
	    -e "s|%msbuild_name%|$(msbuild_name)|" \
	    -e "s|^/cygdrive/\([a-zA-Z]\)|\1:|" \
	< $< > $@

$(ejson_vcproj): $(ejson_vcproj).tpl $(WIN_ERL_INCLUDE)
	sed -e "s|%ERLANG_INCLUDE%|`cat $(WIN_ERL_INCLUDE)`|" \
	< $< > $@

$(WIN_ERL_INCLUDE):$(ERLANG_INCLUDE)
	echo $< | sed -e "s|^/cygdrive/\([a-zA-Z]\)|\1:|" > $@
endif

%.app: %.app.in
	cp $< $@

%.beam: %.erl
	$(ERLC) $(ERLC_FLAGS) $<
