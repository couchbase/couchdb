/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

var Context = function(parent) {
    var self = this;

    self.parent = parent;
    self.variables = {};

    // map of used
    self.used = {};
};

Context.prototype.set = function(name, loc) {
    var self = this;

    // in javascript you can use variables before they are declared
    // yea, it is magical
    if (self.used[name]) {
        return;
    }

    self.variables[name] = loc;
};

Context.prototype.get = function(name) {
    var self = this;

    var val = self.variables[name];
    if (val) {
        return val;
    }

    if (!self.parent) {
        return;
    }

    return self.parent.get(name);
};

Context.prototype.remove = function(name) {
    var self = this;

    self.used[name] = true;

    // if in our scope, remove
    // otherwise we will start checking parent scope
    if (self.variables[name]) {
        return delete self.variables[name];
    }

    if (!self.parent) {
        return;
    }

    self.parent.remove(name);
};

// return list of the unused variables in this context
Context.prototype.unused = function() {
    var self = this;
    return Object.keys(self.variables).map(function(key) {
        return self.variables[key];
    });
};

module.exports = Context;
