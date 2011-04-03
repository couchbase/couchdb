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

SUBDIRS = google-snappy

snappyebindir = $(localerlanglibdir)/snappy-1.0.1/ebin
snappyprivdir = $(localerlanglibdir)/snappy-1.0.1/priv

CLEANFILES = \
	$(snappyebin_make_generated_file_list) \
	$(snappypriv_make_generated_file_list)

snappypriv_LTLIBRARIES = snappy.la

SNAPPY_CXX_SRCS = \
	snappy.cc

SNAPPY_OBJECTS = \
	google-snappy/snappy.o \
	google-snappy/snappy-sinksource.o \
	google-snappy/snappy-stubs-internal.o

snappy_la_SOURCES = $(SNAPPY_CXX_SRCS)
snappy_la_LDFLAGS = -module -avoid-version
snappy_la_LIBADD = $(SNAPPY_OBJECTS)

if WINDOWS
snappy_la_LDFLAGS += -no-undefined
endif

snappy_file_collection = \
    snappy.app.in \
    snappy.erl \
    $(SNAPPY_CXX_SRCS)

snappyebin_make_generated_file_list = \
    snappy.app \
    snappy.beam

snappyebin_DATA = \
    $(snappyebin_make_generated_file_list)

EXTRA_DIST =  \
    $(snappy_file_collection) \
    erl_nif_compat.h \
    priv

priv/snappy.so: .libs/snappy.so
	$(LN_S) .libs priv

all: priv/snappy.so

%.app: %.app.in
	cp $< $@

%.beam: %.erl
	$(ERLC) $(ERLC_FLAGS) $<
