#pragma once

#ifndef _CONFIG_STATIC_H
#define _CONFIG_STATIC_H

#ifdef WIN32
#define __WIN32__
// Including SDKDDKVer.h defines the highest available Windows platform.

// If you wish to build your application for a previous Windows platform, include WinSDKVer.h and
// set the _WIN32_WINNT macro to the platform you wish to support before including SDKDDKVer.h.

#include <SDKDDKVer.h>

#define WIN32_LEAN_AND_MEAN             // Exclude rarely-used stuff from Windows headers
#define MAXPATHLEN 1024
#include <windows.h>
#include <stdio.h>
#include <stdint.h>

#define inline __inline

#define ssize_t long
#define off_t   long

static inline ssize_t pread(int fd, void *buf, size_t count, off_t offset)
{
    off_t pos = lseek(fd, offset, SEEK_SET);
    if (pos < 0) {
        return pos;
    }
    return read(fd, buf, count);
}
#else
#define HAVE_SYS_PARAM_H 1
#define HAVE_UNISTD_H 1
#define HAVE_NETINET_IN_H 1

#endif
#endif // _CONFIG_STATIC_H
