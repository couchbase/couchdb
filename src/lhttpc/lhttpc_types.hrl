%%% ----------------------------------------------------------------------------
%%% Copyright (c) 2009, Erlang Training and Consulting Ltd.
%%% All rights reserved.
%%% 
%%% Redistribution and use in source and binary forms, with or without
%%% modification, are permitted provided that the following conditions are met:
%%%    * Redistributions of source code must retain the above copyright
%%%      notice, this list of conditions and the following disclaimer.
%%%    * Redistributions in binary form must reproduce the above copyright
%%%      notice, this list of conditions and the following disclaimer in the
%%%      documentation and/or other materials provided with the distribution.
%%%    * Neither the name of Erlang Training and Consulting Ltd. nor the
%%%      names of its contributors may be used to endorse or promote products
%%%      derived from this software without specific prior written permission.
%%% 
%%% THIS SOFTWARE IS PROVIDED BY Erlang Training and Consulting Ltd. ''AS IS''
%%% AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
%%% IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
%%% ARE DISCLAIMED. IN NO EVENT SHALL Erlang Training and Consulting Ltd. BE
%%% LIABLE SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
%%% BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
%%% WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
%%% OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
%%% ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
%%% ----------------------------------------------------------------------------

-type header() :: {string() | atom(), string()}.
-type headers() :: [header()].

-type socket() :: _.

-type invalid_option() :: any().

-type partial_download_option() ::
        {window_size, window_size()} |
        {part_size, non_neg_integer() | infinity} |
        invalid_option().

-type option() ::
        {connect_timeout, timeout()} |
        {send_retry, non_neg_integer()} |
        {partial_upload, non_neg_integer() | infinity} |
        {partial_download, [partial_download_option()]} |
        {connect_options, socket_options()} |
        {proxy, string()} |
        {proxy_ssl_options, socket_options()} |
        {pool, pid() | atom()} |
        invalid_option().

-type options() :: [option()].

-type host() :: string() | {integer(), integer(), integer(), integer()}.

-type socket_options() :: [{atom(), term()} | atom()].

-type window_size() :: non_neg_integer() | infinity.

