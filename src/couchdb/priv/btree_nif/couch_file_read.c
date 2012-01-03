//This mostly comes from couch_file.erl

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <netinet/in.h>
#include "snappy-c.h"
#include "ei.h"


#define SIZE_BLOCK 4096

ssize_t total_read_len(off_t blockoffset, ssize_t finallen)
{
    ssize_t left;
    ssize_t add = 0;
    if(blockoffset == 0)
    {
        add++;
        blockoffset = 1;
    }

    left = SIZE_BLOCK - blockoffset;
    if(left >= finallen)
    {
        return finallen + add;
    }
    else
    {
        if((finallen - left) % (SIZE_BLOCK - 1) != 0)
        {
            add++;
        }
        return finallen + add + ((finallen - left) / (SIZE_BLOCK - 1));
    }
}

int remove_block_prefixes(char* buf, off_t offset, ssize_t len)
{
    off_t buf_pos = 0;
    off_t gap = 0;
    ssize_t remain_block;
    while(buf_pos + gap < len)
    {
        remain_block = SIZE_BLOCK - offset;
        if(remain_block > (len - gap - buf_pos))
        {
            remain_block = len - gap - buf_pos;
        }

        if(offset == 0)
        {
            gap++;
            //printf("Move %d bytes <-- by %d, landing at %d\r\n", remain_block, gap, buf_pos);
            memmove(buf + buf_pos, buf + buf_pos + gap, remain_block);
            offset = 1;
        }
        else
        {
            buf_pos += remain_block;
            offset = 0;
        }
    }
    return len - gap;
}

// Sets *dst to returned buffer, returns end size.
// Increases pos by read len.
int raw_read(int fd, off_t *pos, ssize_t len, char** dst)
{
    off_t blockoffs = *pos % SIZE_BLOCK;
    ssize_t total = total_read_len(blockoffs, len);
    *dst = (char*) malloc(total);
    if(!*dst)
    {
        return -1;
    }
    ssize_t got_bytes = pread(fd, *dst, total, *pos);
    if(got_bytes <= 0)
        goto fail;

    *pos += got_bytes;
    return remove_block_prefixes(*dst, blockoffs, got_bytes);

fail:
    free(*dst);
    return -1;
}

int pread_bin_int(int fd, off_t pos, char **ret_ptr)
{
   char *bufptr, *bufptr_rest, *newbufptr;
   char prefix;
   int buf_len;
   uint32_t chunk_len;
   int skip = 0;
   buf_len = raw_read(fd, &pos, 2*SIZE_BLOCK - (pos % SIZE_BLOCK), &bufptr);
   //buf_len = raw_read(fd, &pos, 10, &bufptr);
   if(buf_len == -1)
   {
       buf_len = raw_read(fd, &pos, 4, &bufptr);
       if(buf_len == -1)
       {
           goto fail;
       }
   }

   prefix = bufptr[0] & 0x80;
   memcpy(&chunk_len, bufptr, 4);
   chunk_len = ntohl(chunk_len) & 0x7FFFFFFF;
   skip += 4;
   if(prefix)
   {
       //Just skip over the md5.. for now.
       skip += 16;
   }

   buf_len -= skip;
   memmove(bufptr, bufptr+skip, buf_len);
   if(chunk_len <= buf_len)
   {
       newbufptr = (char*) realloc(bufptr, chunk_len);
       if(!newbufptr)
       {
           free(bufptr);
           goto fail;
       }
       bufptr = newbufptr;
       *ret_ptr = bufptr;
       return chunk_len;
   }
   else
   {
       int rest_len = raw_read(fd, &pos, chunk_len - buf_len, &bufptr_rest);
       if(rest_len == -1)
       {
           free(bufptr);
           goto fail;
       }
       newbufptr = (char*) realloc(bufptr, buf_len + rest_len);
       if(!newbufptr)
       {
           free(bufptr);
           goto fail;
       }
       bufptr = newbufptr;
       memcpy(bufptr + buf_len, bufptr_rest, rest_len);
       free(bufptr_rest);
       *ret_ptr = bufptr;
       return chunk_len;
   }

fail:
   return -1;
}

int pread_bin(int fd, off_t pos, char **ret_ptr)
{
    char *new_buf;
    int len = pread_bin_int(fd, pos, ret_ptr);
    if(len < 0)
    {
        return len;
    }
    size_t new_len;
    if((*ret_ptr)[0] == 1) //Snappy
    {
        if(snappy_uncompressed_length((*ret_ptr) + 1, len - 1, &new_len)
                != SNAPPY_OK)
        {
            //marked as compressed but snappy doesn't see it as valid.
            free(*ret_ptr);
            return -1;
        }

        new_buf = (char*) malloc(new_len);
        if(!new_buf)
        {
            free(*ret_ptr);
            return -1;
        }

        snappy_status ss = (snappy_uncompress((*ret_ptr) + 1, len - 1, new_buf, &new_len));
        if(ss == SNAPPY_OK)
        {
            free(*ret_ptr);
            *ret_ptr = new_buf;
            return new_len;
        }
        else
        {
            free(*ret_ptr);
            return -1;
        }
    }
    else
    {
        return len;
    }
}

