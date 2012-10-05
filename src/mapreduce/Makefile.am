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

MAPREDUCE_VERSION = 1.0
mapreduceebindir = $(localerlanglibdir)/mapreduce-$(MAPREDUCE_VERSION)/ebin
mapreduceprivdir = $(localerlanglibdir)/mapreduce-$(MAPREDUCE_VERSION)/priv

if WINDOWS
MAPREDUCE_OUTPUT_DIR = win32
mapreduce_msbuild = $(MAPREDUCE_OUTPUT_DIR)/msbuild.bat
mapreduce_vcproj = $(MAPREDUCE_OUTPUT_DIR)/mapreduce.vcxproj
WIN_ERL_INCLUDE = $(MAPREDUCE_OUTPUT_DIR)/win_erl_include.tmp
MAPREDUCE_NIF = $(MAPREDUCE_OUTPUT_DIR)/mapreduce_nif.dll
WIN_V8_INCLUDE = $(MAPREDUCE_OUTPUT_DIR)/win_v8_include.tmp
WIN_V8_LIB = $(MAPREDUCE_OUTPUT_DIR)/win_v8_lib.tmp
else
mapreduce_cxx_srcs = \
    mapreduce_nif.cc \
    mapreduce.cc

mapreduce_cxx_hdrs = \
    erl_nif_compat.h \
    mapreduce.h
endif
mapreduce_file_collection = \
    mapreduce.app.in \
    mapreduce.erl

mapreduceebin_make_generated_file_list = \
    mapreduce.app \
    mapreduce.beam

test_files = \
    test/01-map.t \
    test/02-reduce.t \
    test/03-builtin-functions.t

EXTRA_DIST = \
    $(mapreduce_cxx_hdrs) \
    $(mapreduce_file_collection) \
    $(test_files) \
    test/run.tpl

CLEANFILES = \
    $(mapreduceebin_make_generated_file_list) \
    priv/mapreduce_nif.so

noinst_SCRIPTS = test/run

if WINDOWS
mapreduceebin_DATA = $(MAPREDUCE_NIF) \
    $(mapreduceebin_make_generated_file_list)
else
mapreduceebin_DATA = \
    $(mapreduceebin_make_generated_file_list)
endif

if !WINDOWS
if USE_OTP_NIFS
mapreducepriv_LTLIBRARIES = mapreduce_nif.la
mapreduce_nif_la_AM_CPPFLAGS = $(AM_CPPFLAGS) -Imapreduce-$(MAPREDUCE_VERSION) $(V8_FLAGS) -Wall
if GCC
mapreduce_nif_la_CPPFLAGS = -Wall -Winline
endif
mapreduce_nif_la_SOURCES = $(mapreduce_cxx_srcs)
mapreduce_nif_la_LDFLAGS = -module -avoid-version $(V8_LIB_FLAGS)

priv/mapreduce_nif.so: mapreduce_nif.la
	@mkdir -p ./priv
	cp .libs/mapreduce_nif.so $@

all: priv/mapreduce_nif.so
endif
endif

if WINDOWS
$(MAPREDUCE_NIF) : $(mapreduce_msbuild) $(mapreduce_vcproj)
	$(MKDIR_P) "$(mapreduceprivdir)" || true
	(cd win32 && cmd /c msbuild.bat)
	cp $(MAPREDUCE_NIF) $(mapreduceprivdir)

all: $(MAPREDUCE_NIF)

$(mapreduce_msbuild): $(mapreduce_msbuild).tpl
	sed -e "s|%msbuild_dir%|$(msbuild_dir)|" \
	    -e "s|%msbuild_name%|$(msbuild_name)|" \
	    -e "s|^/cygdrive/\([a-zA-Z]\)|\1:|" \
	< $< > $@

$(mapreduce_vcproj): $(mapreduce_vcproj).tpl $(WIN_ERL_INCLUDE) $(WIN_V8_INCLUDE) $(WIN_V8_LIB)
	 sed -e "s|%ERLANG_INCLUDE%|`cat $(WIN_ERL_INCLUDE)`|" \
	     -e "s|%V8_INCLUDE%|`cat $(WIN_V8_INCLUDE)`|" \
	     -e "s|%V8_LIB_DIR%|`cat $(WIN_V8_LIB)`|" \
	     -e "s|%MAPREDUCE_VERSION%|$(MAPREDUCE_VERSION)|" \
	< $< > $@

$(WIN_ERL_INCLUDE):$(ERLANG_INCLUDE)
	echo $< | sed -e "s|^/cygdrive/\([a-zA-Z]\)|\1:|" > $@

$(WIN_V8_INCLUDE):$(V8_INCLUDE)
	echo $< | sed -e "s|^/cygdrive/\([a-zA-Z]\)|\1:|" > $@

$(WIN_V8_LIB):$(V8_LIB_DIR)
	echo $< | sed -e "s|^/cygdrive/\([a-zA-Z]\)|\1:|" > $@
endif

check:
	$(abs_top_builddir)/src/mapreduce/test/run $(abs_top_srcdir)/src/mapreduce/test

test/run: test/run.tpl
	sed -e "s|%abs_top_srcdir%|@abs_top_srcdir@|g" \
	    -e "s|%abs_top_builddir%|@abs_top_builddir@|g" > \
	$@ < $<
	chmod +x $@

%.app: %.app.in
	cp $< $@

%.beam: %.erl
	$(ERLC) $(ERLC_FLAGS) $<
