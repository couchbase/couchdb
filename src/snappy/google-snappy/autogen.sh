#! /bin/sh -e
export PATH=/bin:/usr/bin  # Local hack.
rm -rf autom4te.cache
aclocal -I m4
autoheader -f
libtoolize -f --copy --automake
automake -f --add-missing --copy
autoconf -f
