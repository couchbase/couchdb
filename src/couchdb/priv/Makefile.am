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

couchlibdir = $(localerlanglibdir)/couch-$(version)
couchprivdir = $(couchlibdir)/priv
couchprivlibdir = $(couchlibdir)/priv/lib

EXTRA_DIST = \
	spawnkillable/couchspawnkillable.sh \
	stat_descriptions.cfg.in

CLEANFILES = stat_descriptions.cfg

if MINGW
ICU_OUTPUT_DIR = icu_driver/win32
icu_driver_msbuild = $(ICU_OUTPUT_DIR)/msbuild.bat
icu_driver_vcproj = $(ICU_OUTPUT_DIR)/couch_icu_driver.vcxproj
WIN_ICU_INCLUDE = $(ICU_OUTPUT_DIR)/win_icu_include.tmp
WIN_ICU_LIB_DIR = $(ICU_OUTPUT_DIR)/win_icu_dir.tmp
WIN_ERL_INCLUDE = $(ICU_OUTPUT_DIR)/win_erl_include.tmp
COUCH_ICU_DRIVER = $(ICU_OUTPUT_DIR)/couch_icu_driver.dll
endif

if !MINGW
ICU_LOCAL_FLAGS = $(ICU_LOCAL_CFLAGS) $(ICU_LOCAL_LDFLAGS)
if WINDOWS
ICU_LOCAL_LIBS=-licuuc -licudt -licuin
else
ICU_LOCAL_LIBS=-licuuc -licudata -licui18n
endif

couchprivlib_LTLIBRARIES = couch_icu_driver.la
couch_icu_driver_la_SOURCES = icu_driver/couch_icu_driver.c
couch_icu_driver_la_LDFLAGS = -module -avoid-version $(ICU_LOCAL_FLAGS)
couch_icu_driver_la_CFLAGS = $(ICU_LOCAL_FLAGS)
couch_icu_driver_la_LIBADD = $(ICU_LOCAL_LIBS)

if WINDOWS
couch_icu_driver_la_LDFLAGS += -no-undefined
endif
endif

if MINGW
couchjs_msbuild = couch_js/win32/msbuild.bat
couchjs_vcproj = couch_js/win32/couchjs.vcxproj
WIN_JS_INCLUDE = couch_js/win32/win_js_include.tmp
WIN_JS_LIB_DIR = couch_js/win32/win_js_dir.tmp
endif

COUCHJS_SRCS = \
	couch_js/http.c \
	couch_js/http.h \
	couch_js/main.c \
	couch_js/utf8.c \
	couch_js/utf8.h

locallibbin_PROGRAMS = couchjs
couchjs_SOURCES = $(COUCHJS_SRCS)
couchjs_LDFLAGS = $(CURL_LDFLAGS)
couchjs_CFLAGS = -D_BSD_SOURCE $(CURL_CFLAGS)
couchjs_LDADD = $(CURL_LDFLAGS) @JSLIB@

couchpriv_DATA = stat_descriptions.cfg
couchpriv_PROGRAMS = couchspawnkillable

%.cfg: %.cfg.in
	cp $< $@

if WINDOWS
couchspawnkillable_SOURCES = spawnkillable/couchspawnkillable_win.c
endif

if !WINDOWS
couchspawnkillable: spawnkillable/couchspawnkillable.sh
	cp $< $@
	chmod +x $@
endif

if MINGW
$(COUCH_ICU_DRIVER) : icu_driver/couch_icu_driver.c $(icu_driver_msbuild) $(icu_driver_vcproj)
	$(MKDIR_P) "$(couchprivlibdir)" || true
	(cd icu_driver/win32 && msbuild.bat)
	cp $(ICU_OUTPUT_DIR)/couch_icu_driver.dll $(couchprivlibdir)
	cp $(ICU_OUTPUT_DIR)/couch_icu_driver.lib $(couchprivlibdir)

all: $(COUCH_ICU_DRIVER)

$(icu_driver_msbuild): $(icu_driver_msbuild).tpl
	sed -e "s|%msbuild_dir%|$(msbuild_dir)|" \
	    -e "s|%msbuild_name%|$(msbuild_name)|" \
	< $< > $@

$(icu_driver_vcproj): $(icu_driver_vcproj).tpl $(WIN_ICU_INCLUDE) $(WIN_ICU_LIB_DIR) $(WIN_ERL_INCLUDE)
	sed -e "s|%ICU_INCLUDE%|`cat $(WIN_ICU_INCLUDE)`|" \
	    -e "s|%ERLANG_INCLUDE%|`cat $(WIN_ERL_INCLUDE)`|" \
	    -e "s|%ICU_LIB_DIR%|`cat $(WIN_ICU_LIB_DIR)`|" \
	< $< > $@

$(WIN_ICU_INCLUDE):$(ICU_INCLUDE)
	echo $< | sed -e "s|^/\([a-zA-Z]\)|\1:|" > $@

$(WIN_ICU_LIB_DIR):$(ICU_LIB_DIR)
	echo $< | sed -e "s|^/\([a-zA-Z]\)|\1:|" > $@

$(WIN_ERL_INCLUDE):$(ERLANG_INCLUDE)
	echo $< | sed -e "s|^/\([a-zA-Z]\)|\1:|" > $@

couchjs$(EXEEXT): $(COUCHJS_SRCS) $(couchjs_msbuild) $(couchjs_vcproj)
	@rm -f couchjs$(EXEEXT)
	(cd couch_js/win32 && msbuild.bat)

$(couchjs_msbuild): $(couchjs_msbuild).tpl
	sed -e "s|%msbuild_dir%|$(msbuild_dir)|" \
	    -e "s|%msbuild_name%|$(msbuild_name)|" \
	< $< > $@

$(couchjs_vcproj): $(couchjs_vcproj).tpl $(WIN_JS_INCLUDE) $(WIN_JS_LIB_DIR)
	sed -e "s|%JS_INCLUDE%|`cat $(WIN_JS_INCLUDE)`|" \
	    -e "s|%JS_LIB_DIR%|`cat $(WIN_JS_LIB_DIR)`|" \
	< $< > $@

$(WIN_JS_INCLUDE):$(JS_INCLUDE)
	echo $< | sed -e "s|^/\([a-zA-Z]\)|\1:|" > $@

$(WIN_JS_LIB_DIR):$(JS_LIB_DIR)
	echo $< | sed -e "s|^/\([a-zA-Z]\)|\1:|" > $@
endif

# libtool and automake have defeated markh.  For each of our executables
# we end up with 2 copies - one directly in the 'target' folder (eg, 'priv')
# and another - the correct one - in .libs.  The former doesn't work but is 
# what gets installed for 'couchspawnkillable' - but the correct one for
# couchjs.exe *does* get copied.  *shrug*  So just clobber it with the 
# correct one as the last step. See bug COUCHDB-439
install-data-hook:
	if test -f "$(DESTDIR)$(couchprivlibdir)/couch_icu_driver"; then \
	    rm -f "$(DESTDIR)$(couchprivlibdir)/couch_icu_driver.so"; \
	    cd "$(DESTDIR)$(couchprivlibdir)" && \
	        $(LN_S) couch_icu_driver couch_icu_driver.so; \
	fi
if WINDOWS
	$(INSTALL) $(ICU_LOCAL_BIN)/icuuc42.dll $(bindir)
	$(INSTALL) $(ICU_LOCAL_BIN)/icudt42.dll $(bindir)
	$(INSTALL) $(ICU_LOCAL_BIN)/icuin42.dll $(bindir)
	$(INSTALL) $(JS_LIB_BINARY) $(bindir)
	$(INSTALL) .libs/couchspawnkillable.exe \
		"$(DESTDIR)$(couchprivdir)/couchspawnkillable.exe"
	$(INSTALL) couchjs$(EXEEXT) $(bindir)
endif

uninstall-local:
	if test -f "$(DESTDIR)$(couchprivlibdir)/couch_erl_driver"; then \
	    rm -f "$(DESTDIR)$(couchprivlibdir)/couch_erl_driver.so"; \
	fi
