/**
 * @copyright 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/

/*
This function calls esprima and unused functions to get AST
and unused variables of map functions (in our view definitions)
It will check if the first parameter which is document in map
function is used in the code or not and returns true, if
document is unused and false otherwise
*/
function is_doc_unused(src) {
    var unused_retval = unused(src);

    for (i = 0; i < unused_retval.unused_vars.length; i++) {
        if (unused_retval.ast.body[0].expression.params[0].name ==
                unused_retval.unused_vars[i].name &&
                unused_retval.unused_vars[i].param == true) {
            return true;
        }
    }

    return false;
}
