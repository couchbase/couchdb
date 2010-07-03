% Copyright 2010 Relaxed, Inc.

-module(couch_httpd_hosting_auth).

-include("couch_db.hrl").
-export([hosting_authentication_handler/1]).

hosting_authentication_handler(Req) ->
    case couch_httpd:header_value(Req, "Authorization") of
        "Basic " ++ Base64Value ->
            case os:getenv("COUCH_HOSTING_CREDENTIALS") of
              false ->
                  Req;
              Credentials ->
                  HostingCredentials = ?l2b(Credentials),
                  case base64:decode(Base64Value) of
                      HostingCredentials ->
                          Req#httpd{user_ctx=#user_ctx{roles=[<<"_admin">>]}};
                      _ ->
                          Req
                  end
            end;
        _ ->
            Req
    end.
