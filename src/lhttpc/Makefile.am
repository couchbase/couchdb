## Licensed under the Apache License, Version 2.0 (the "License"); you may not
## use this file except in compliance with the License. You may obtain a copy of
## the License at
##
##   http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
## WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
## License for the specific language governing permissions and limitations under
## the License.

lhttpcebindir = $(localerlanglibdir)/lhttpc-1.3/ebin

lhttpc_file_collection = \
	lhttpc.app.src \
    lhttpc.erl \
    lhttpc_client.erl \
    lhttpc_lib.erl \
    lhttpc_manager.erl \
    lhttpc_sock.erl \
    lhttpc_sup.erl

lhttpcebin_make_generated_file_list = \
	lhttpc.app \
    lhttpc.beam \
    lhttpc_client.beam \
    lhttpc_lib.beam \
    lhttpc_manager.beam \
    lhttpc_sock.beam \
    lhttpc_sup.beam

lhttpcebin_DATA = \
    $(lhttpcebin_make_generated_file_list)

EXTRA_DIST =  \
    $(lhttpc_file_collection) \
    lhttpc_types.hrl \
    lhttpc.hrl

CLEANFILES = \
    $(lhttpcebin_make_generated_file_list)

%.app: %.app.src
	cp $< $@

%.beam: %.erl
	$(ERLC) $(ERLC_FLAGS) $<
