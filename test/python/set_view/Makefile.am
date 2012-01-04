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

noinst_SCRIPTS = run.py

EXTRA_DIST = \
	run.py.tpl \
	burst_state_updates.py \
	cleanup.py \
	compaction.py \
	compaction_transitions.py \
	erlang_views.py \
	filter_partitions.py \
	include_docs.py \
	many_partitions.py \
	passive_partitions.py \
	passive_partitions_update.py \
	replica_index.py \
	stale.py \
	update_cleanup.py \
	updates.py \
	view_merge.py \
	view_params.py \
	common/common.py

DISTCLEANFILES = *.pyc common/*.pyc

CLEANFILES = run.py

run.py: run.py.tpl
	sed -e "s|%abs_top_srcdir%|@abs_top_srcdir@|g" \
	    -e "s|%abs_top_builddir%|@abs_top_builddir@|g" > \
	$@ < $<
	chmod +x $@

