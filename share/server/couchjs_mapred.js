// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

var server_config = {};
var map_funs = [];
var red_funs = {};
var map_results = [];
var line_length = null;
var sandbox = null;

var handle_error = function(e) {
  var type = e[0];
  if (type == "fatal") {
    e[0] = "error"; // we tell the client it was a fatal error by dying
    respond(e);
    quit(-1);
  } else if (type == "error") {
    respond(e);
  } else if (e.error && e.reason) {
    // compatibility with old error format
    respond(["error", e.error, e.reason]);
  } else {
    respond(["error","unnamed_error",e.toSource()]);
  }
};

var handle_view_error = function(err, doc) {
  if (err == "fatal_error") {
    // Only if it's a "fatal_error" do we exit. What's a fatal error?
    // That's for the query to decide.
    //
    // This will make it possible for queries to completely error out,
    // by catching their own local exception and rethrowing a
    // fatal_error. But by default if they don't do error handling we
    // just eat the exception and carry on.
    //
    // In this case we abort map processing but don't destroy the 
    // JavaScript process. If you need to destroy the JavaScript 
    // process, throw the error form matched by the block below.
    throw(["error", "map_runtime_error", "function raised 'fatal_error'"]);
  } else if (err[0] == "fatal") {
    // Throwing errors of the form ["fatal","error_key","reason"]
    // will kill the OS process. This is not normally what you want.
    throw(err);
  }
  var message = "function raised exception " + err.toSource();
  if (doc) message += " with doc._id " + doc._id;
  log(message);
};

var emit = function(key, value) {
    map_results[map_results.length-1].push([key, value]);
};

var sum = function(values) {
  var rv = 0;
  for(var i in values) {
    rv += values[i];
  }
  return rv;
};

var respond = function(obj) {
  try {
    print(JSON.stringify(obj));
  } catch(e) {
    print(JSON.stringify(["log", "Error converting object to JSON: " + e.toString()]));
    print(JSON.stringify(["log", "Error on object: " + obj.toSource()]));
  }
};

var log = function(message) {
  if(typeof(message) != "string") {
    message = JSON.stringify(message);
  }
  respond(["log", message]);
};

var compile_function = function(source) {    
  if (!source) throw(["error","not_found","missing function"]);
  var func = undefined;
  try {
    if (sandbox) {
      var func = evalcx(source, sandbox);
    } else {
      var func = eval(source);
    }
  } catch (err) {
    throw(["error", "compilation_error", err.toSource() + " (" + source + ")"]);
  }
  if(typeof(func) == "function") {
    return func;
  } else {
    throw(["error","compilation_error",
      "Expression does not eval to a function. (" + source.toSource() + ")"]);
  }
};

var init_sandbox = function() {
  try {
    // if possible, use evalcx (not always available)
    sandbox = evalcx('');
    sandbox.emit = emit;
    sandbox.sum = sum;
    sandbox.log = log;
    sandbox.toJSON = JSON.stringify;
    sandbox.JSON = JSON;
  } catch (e) {
    log(e.toSource());
  }
};

var configure = function(config) {
  server_config = config || {};
  respond(true);
}

var reset = function() {
  map_funs = new Array();
  red_funs = {};
  map_results = new Array();
  sandbox = null;
  init_sandbox();
  respond(true);
};

var compile = function(maps, reds) {
  try {
    map_funs = new Array(maps.length);
    for(var i = 0; i < maps.length; i++) {
      map_funs[i] = compile_function(maps[i]);
    }
    red_funs = {};
    for(var i = 0; i < reds.length; i++) {
      var view_id = reds[i][0];
      red_funs[view_id] = new Array(reds[i][1].length);
      for(var j = 0; j < reds[i][1].length; j++) {
        red_funs[view_id][j] = compile_function(reds[i][1][j]);
      }
    }
    respond(true);
  } catch(e) {
    handle_error(e);
  }
};

var map = function(doc) {
  map_results = new Array();
  map_funs.forEach(function(fun) {
    map_results.push([]);
    try {
      fun(doc);
    } catch(e) {
      handle_view_error(e);
      map_results[map_results.length-1] = [];
    }
  });
  respond([true, map_results]);
};

var check_reductions = function(reductions) {
  var reduce_line = JSON.stringify(reductions);
  var reduce_length = reduce_line.length;
  if (server_config && server_config.reduce_limit &&
          reduce_length > 200 && ((reduce_length * 2) > line_length)) {
    var reduce_preview = "Current output: '" + reduce_line.substring(0, 100) +
                        "'... (first 100 of " + reduce_length + " bytes)";
    throw(["error", "reduce_overflow_error",
            "Reduce output must shrink more rapidly: " + reduce_preview]);
  } else {
    print("[true," + reduce_line + "]");
  }
}

var reduce = function(view_id, kvs) {
  var keys = new Array(kvs.length);
  var vals = new Array(kvs.length);
  for(var i = 0; i < kvs.length; i++) {
      keys[i] = kvs[i][0];
      vals[i] = kvs[i][1];
  }
  var reductions = new Array(red_funs[view_id].length);
  for(var i = 0; i < red_funs[view_id].length; i++) {
      reductions[i] = red_funs[view_id][i](keys, vals, false);
  }
  if(keys.length > 1) {
    check_reductions(reductions);
  } else {
    respond([true, reductions]);
  }
}

var rereduce = function(view_id, vals) {
  var reductions = new Array(red_funs[view_id].length);
  for(var i = 0; i < red_funs[view_id].length; i++) {
    reductions[i] = red_funs[view_id][i](null, vals[i], true);
  }
  if(vals.length > 1) {
    check_reductions(reductions);
  } else {
    respond([true, reductions]);
  }
}

var commands = {
  "configure": configure,
  "reset": reset,
  "compile": compile,
  "map": map,
  "reduce": reduce,
  "rereduce": rereduce
};

init_sandbox();

while(line = readline()) {
  cmd = eval('('+line+')');
  line_length = line.length;
  try {
    cmdkey = cmd.shift();
    if(commands[cmdkey]) {
      commands[cmdkey].apply(null, cmd);
    } else {
      throw(["fatal", "unknown_command", "unknown command '" + cmdkey + "'"]);
    }
  } catch(e) {
    handle_error(e);
  }
}
