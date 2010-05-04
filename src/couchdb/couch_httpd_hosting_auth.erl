% Copyright 2010 Relaxed, Inc.

-module(couch_httpd_hosting_auth).

-include("couch_db.hrl").
-export([hosting_authentication_handler/1]).

hosting_authentication_handler(Req) ->
    Req#httpd{user_ctx=#user_ctx{roles=[<<"_admin">>]}}.
