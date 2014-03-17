#ifndef EJSON_H
#define EJSON_H

ERL_NIF_TERM
validate_doc(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

ERL_NIF_TERM
reverse_tokens(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

ERL_NIF_TERM
final_encode(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

#endif
