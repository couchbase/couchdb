ACLOCAL_AMFLAGS = -I m4

# Library.
noinst_LTLIBRARIES = libsnappy.la
libsnappy_la_SOURCES = snappy.cc snappy-sinksource.cc snappy-stubs-internal.cc
libsnappy_la_LDFLAGS = -version-info $(SNAPPY_LTVERSION)

include_HEADERS = snappy.h snappy-sinksource.h snappy-stubs-public.h
noinst_HEADERS = snappy-internal.h snappy-stubs-internal.h

dist_doc_DATA = AUTHORS ChangeLog COPYING NEWS README

libtool: $(LIBTOOL_DEPS)
	$(SHELL) ./config.status --recheck
