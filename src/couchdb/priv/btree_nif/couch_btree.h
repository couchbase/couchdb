#include <unistd.h>
#include <stdint.h>

#include "erl_nif.h"

#define SIZE_BLOCK 4096

typedef struct compare_info {
    //used by find_first_gteq
    int last_cmp_val;
    void* last_cmp_key;
    int list_pos;
    //Compare function
    int (*compare)(void *k1, void *k2);
    //Given an erlang term, return a pointer accepted by the compare function
    void *(*from_ext)(struct compare_info* ci, char* buf, int pos);
    void *arg;
} compare_info;

typedef struct eterm_buf {
    char* buf;
    size_t size;
} eterm_buf;

typedef struct couchfile_leaf_value {
    eterm_buf term;
} couchfile_leaf_value;

typedef struct couchfile_pointer_info {
    eterm_buf key;
    uint64_t pointer;
    void* writerq_resource;
    eterm_buf reduce_value;
    uint64_t subtreesize;
} couchfile_pointer_info;

typedef struct nif_writerq {
    couchfile_pointer_info *ptr;
    ssize_t size;
    char buf[1];
} nif_writerq;

typedef struct nodelist {
    union value {
        couchfile_leaf_value *leaf;
        couchfile_pointer_info *pointer;
        void *mem;
    } value;
    struct nodelist *next;
} nodelist;

#define ACTION_FETCH  0
#define ACTION_INSERT 1
#define ACTION_REMOVE 2

typedef struct couchfile_modify_action {
    int type;
    eterm_buf *key;
    void* cmp_key;
    eterm_buf *value;
} couchfile_modify_action;

typedef struct worker_cond {
    ErlNifCond* cond;
    ErlNifMutex* mtx;
} worker_cond;

typedef struct btreenif_state {
    worker_cond writer_cond;
    worker_cond work_queue_cond;
    ErlNifEnv* check_env;
    ERL_NIF_TERM atom_heart;
    void* queue;
} btreenif_state;

typedef struct couchfile_modify_request {
    compare_info cmp;
    int fd;
    int num_actions;
    couchfile_modify_action* actions;
    void (*fetch_callback) (struct couchfile_modify_request *rq, eterm_buf* k, eterm_buf* v);
    //Put result term into the eterm_buf.
    void (*reduce) (eterm_buf* dst, nodelist* leaflist, int count);
    void (*rereduce) (eterm_buf* dst, nodelist* ptrlist, int count);
    ErlNifPid caller;
    ErlNifPid writer;
    ErlNifEnv* nif_env;
    ErlNifEnv* caller_env;
    btreenif_state* globalstate;
    couchfile_pointer_info root;
    ERL_NIF_TERM ref;
    struct couchfile_modify_request* next;
} couchfile_modify_request;

#define KV_NODE 0
#define KP_NODE 1

//Used to build and chunk modified nodes
typedef struct couchfile_modify_result {
    couchfile_modify_request *rq;
    nodelist* values;
    nodelist* values_end;

    long node_len;
    int count;

    nodelist* pointers;
    nodelist* pointers_end;
    //If we run over a node and never set this, it can be left as-is on disk.
    int modified;
    //0 - leaf, 1 - ptr
    int node_type;
    int error_state;
} couchfile_modify_result;

//Read a chunk from file, remove block prefixes, and decompress.
//Don't forget to free when done with the returned value.
//(If it returns -1 it will not have set ret_ptr, no need to free.)
int pread_bin(int fd, off_t pos, char **ret_ptr);

ssize_t total_read_len(off_t blockoffset, ssize_t finallen);

couchfile_pointer_info* modify_btree(couchfile_modify_request *rq,
        couchfile_pointer_info *root, int *errcode);

int append_raw(int fd, char* buf, size_t len);

nif_writerq *nif_writerq_alloc(ssize_t size);
void nif_write(couchfile_modify_request *rq, couchfile_pointer_info *dst, nif_writerq* wrq, ssize_t size);
int wait_pointer(couchfile_modify_request *rq, couchfile_pointer_info *ptr);

extern ErlNifResourceType* writerq_type;

ERL_NIF_TERM get_atom(ErlNifEnv* env, const char *name);

//Errors
#define ERROR_READ_FILE -1
#define ERROR_PARSE -2
#define ERROR_WRITER_DEAD -3
#define ERROR_ALLOC_FAIL -4

