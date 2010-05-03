LIBDIR=$(shell erl -eval 'io:format("~s~n", [code:lib_dir()])' -s init stop -noshell)
.PHONY: doc
VERSION=0.3.4

all:
	mkdir -p ebin
	(cd src;$(MAKE))

#doc:
#	(cd src; $(MAKE) doc)

test: all
	(cd t;$(MAKE))
	(cd t;$(MAKE) test)

clean:
	(cd src;$(MAKE) clean)
	(cd t;$(MAKE) clean)
	rm -rf cover/

#package: clean
#	@mkdir etap-$(VERSION)/ && cp -rf ChangeLog Makefile README.markdown scripts src support t etap-$(VERSION)
#	@COPYFILE_DISABLE=true tar zcf etap-$(VERSION).tgz etap-$(VERSION)
#	@rm -rf etap-$(VERSION)/

#install:
#	mkdir -p $(prefix)/$(LIBDIR)/etap-$(VERSION)/ebin
#	for i in ebin/*.beam; do install $$i $(prefix)/$(LIBDIR)/etap-$(VERSION)/$$i ; done