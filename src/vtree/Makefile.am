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

vtreeebindir = $(localerlanglibdir)/vtree/ebin

vtree_file_collection = \
    vtree.erl \
    vtreeviz.erl \
    run_vtreeviz.erl \
    vtree_test.erl \
    vtree_insbench.erl

vtreeebin_make_generated_file_list = \
    vtree.beam \
    vtreeviz.beam \
    run_vtreeviz.beam
    vtree_test.beaml \
    vtree_insbench.beam

vtreeebin_DATA = \
    $(vtreeebin_make_generated_file_list)

EXTRA_DIST =  $(vtree_file_collection)

CLEANFILES = \
    $(vtreeebin_make_generated_file_list)

%.beam: %.erl
	$(ERLC) $(ERLC_FLAGS) $<
