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

/**
 * This function returns array of properties that match given key
 **/
function getObjects(obj, key) {
    var objects = [];
    for (var prop in obj) {
        if (prop == key ) {
            objects.push(obj[key]);
        }
        else if (typeof obj[prop] == 'object') {
            objects = objects.concat(getObjects(obj[prop], key));
        }
    }
    return objects;
}

/**
 * This function checks if eval function is used or not by checking the names
 * of all `callee` properties in the AST generated for map function
 **/
function is_eval_used(obj) {
    var objects = getObjects(obj, 'callee');
    for (var i = 0; i < objects.length; i++) {
        if (objects[i].name == 'eval') {
            return true;
        }
    }
    return false;
}

/**
 * This function calls esprima and unused functions to get AST
 * and unused variables of map functions (in our view definitions)
 * It will check if the first parameter which is document in map
 * function is used in the code or not as well as if the eval function
 * is used in map body or not. If eval function is used it is difficult
 * to determine if any doc parameters are evaluated or not. In this case
 * this function returns that document parameter is used otherwise it
 * returns if the document parameter is used or not. Specifically it
 * returns true if doc parameter is unused and false otherwise.
 **/
function is_doc_unused(src) {
    var unused_retval = unused(src);

    for (var i = 0; i < unused_retval.unused_vars.length; i++) {
        if (unused_retval.ast.body[0].expression.params[0].name ==
                unused_retval.unused_vars[i].name &&
                unused_retval.unused_vars[i].param == true) {
            return true && !is_eval_used(unused_retval.ast);
        }
    }
    return false && !is_eval_used(unused_retval.ast);
}
