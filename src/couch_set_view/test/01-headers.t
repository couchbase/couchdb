#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable

% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-include_lib("couch_set_view/include/couch_set_view.hrl").

main(_) ->
    test_util:init_code_path(),

    etap:plan(9),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
    ok.


test() ->
    couch_set_view_test_util:start_server(<<"couch_set_view_header_tests">>),

    EmptyHeader = #set_view_index_header{},
    EmptyGroup = #set_view_group{
        sig = couch_util:md5(<<"foobar">>),
        index_header = EmptyHeader
    },
    EmptyHeaderBin = couch_set_view_util:group_to_header_bin(EmptyGroup),
    etap:is(couch_set_view_util:header_bin_sig(EmptyHeaderBin),
            EmptyGroup#set_view_group.sig,
            "Correct signature in serialized empty header"),
    etap:is(couch_set_view_util:header_bin_to_term(EmptyHeaderBin),
            EmptyHeader,
            "Can serialize and deserialize an empty header"),

    Header1 = #set_view_index_header{
        num_partitions = 1024,
        abitmask = couch_set_view_util:build_bitmask(lists:seq(0, 255)),
        pbitmask = couch_set_view_util:build_bitmask(lists:seq(256, 512)),
        cbitmask = couch_set_view_util:build_bitmask(lists:seq(756, 1023)),
        seqs = [{I, I * I * I * I * I} || I <- lists:seq(0, 509)],
        id_btree_state = <<"foobar_id_btree">>,
        view_states = [<<"foobar_view1_btree">>, <<"foobar_view2_btree">>],
        has_replica = true,
        replicas_on_transfer = [600, 701, 702],
        unindexable_seqs = [{510, 1048576}, {511, 1048576012}, {512, 321}]
    },
    Group1 = #set_view_group{
        sig = couch_util:md5(<<"foobar_1">>),
        index_header = Header1
    },
    Header1Bin = couch_set_view_util:group_to_header_bin(Group1),
    etap:is(couch_set_view_util:header_bin_sig(Header1Bin),
            Group1#set_view_group.sig,
            "Correct signature in serialized Header1"),
    etap:is(couch_set_view_util:header_bin_to_term(Header1Bin),
            Header1,
            "Can serialize and deserialize Header1"),

    Header2 = #set_view_index_header{
        num_partitions = 64,
        abitmask = couch_set_view_util:build_bitmask(lists:seq(0, 20)),
        pbitmask = couch_set_view_util:build_bitmask(lists:seq(21, 30)),
        cbitmask = couch_set_view_util:build_bitmask([60, 61, 62, 63]),
        seqs = [{I, 1} || I <- lists:seq(0, 30)],
        id_btree_state = <<"foobar_id_btree">>,
        view_states = [<<"foobar_view1_btree">>, <<"foobar_view2_btree">>],
        has_replica = true,
        replicas_on_transfer = [],
        unindexable_seqs = [{20, 2}, {28, 1048576012}, {29, 1}],
        pending_transition = #set_view_transition{
            active = [60],
            passive = [61, 62, 63]
        }
    },
    Group2 = #set_view_group{
        sig = couch_util:md5(<<"foobar_2">>),
        index_header = Header2
    },
    Header2Bin = couch_set_view_util:group_to_header_bin(Group2),
    etap:is(couch_set_view_util:header_bin_sig(Header2Bin),
            Group2#set_view_group.sig,
            "Correct signature in serialized Header2"),
    etap:is(couch_set_view_util:header_bin_to_term(Header2Bin),
            Header2,
            "Can serialize and deserialize Header2"),

    Header3 = Header2#set_view_index_header{
        pending_transition = #set_view_transition{
            active = [],
            passive = [61, 63]
        }
    },
    Group3 = #set_view_group{
        sig = couch_util:md5(<<"foobar_2">>),
        index_header = Header3
    },
    Header3Bin = couch_set_view_util:group_to_header_bin(Group3),
    etap:is(couch_set_view_util:header_bin_sig(Header3Bin),
            Group3#set_view_group.sig,
            "Correct signature in serialized Header3"),
    etap:is(couch_set_view_util:header_bin_to_term(Header3Bin),
            Header3,
            "Can serialize and deserialize Header3"),

    etap:isnt(Header3Bin,
              Header2Bin,
              "Serialized forms of headers 2 and 3 are different"),
    ok.
