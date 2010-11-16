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

(function($) {  

  $.fn.centerBox = function() {
    return this.each(function() {
      var s = this.style;
      s.left = (($(window).width() - $(this).width()) / 2) + "px";
      s.top = (($(window).height() - $(this).height()) / 2) + "px";
    });
  }

  $.showDialog = function(url, options) {
    options = options || {};
    options.load = options.load || function() {};
    options.cancel = options.cancel || function() {};
    options.validate = options.validate || function() { return true };
    options.submit = options.submit || function() {};

    var overlay = $('<div id="overlay" style="z-index:1001"></div>')
      .css("opacity", "0");
    var dialog = $('<div id="dialog" style="z-index:1002;position:fixed;display:none;"></div>');
    if ($.browser.msie) {
      var frame = $('<iframe id="overlay-frame" style="z-index:1000;border:none;margin:0;padding:0;position:absolute;width:100%;height:100%;top:0;left:0" src="javascript:false"></iframe>')
        .css("opacity", "0").appendTo(document.body);
      if (parseInt($.browser.version)<7) {
        dialog.css("position", "absolute");
        overlay.css("position", "absolute");
        $("html,body").css({width: "100%", height: "100%"});
      }
    }
    overlay.appendTo(document.body).fadeTo(100, 0.6);
    dialog.appendTo(document.body).centerBox().fadeIn(400);

    $(document).keydown(function(e) {
      if (e.keyCode == 27) dismiss(); // dismiss on escape key
    });
    function dismiss() {
      dialog.fadeOut("fast", function() {
        $("#dialog, #overlay, #overlay-frame").remove();
      });
      $(document).unbind("keydown");
    }
    overlay.click(function() { dismiss(); });

    function showError(name, message) {
      var input = dialog.find(":input[name=" + name + "]");
      input.addClass("error").next("div.error").remove();
      $('<div class="error"></div>').text(message).insertAfter(input);
    }

    $.get(url, function(html) {
      $(html).appendTo(dialog);
      dialog.centerBox().each(function() {
        options.load(dialog.children()[0]);
        $(":input:first", dialog).each(function() { this.focus() });
        $("button.cancel", dialog).click(function() { // dismiss on cancel
          dismiss();
          options.cancel();
        });
        $("form", dialog).submit(function(e) { // invoke callback on submit
          e.preventDefault();
          dialog.find("div.error").remove().end().find(".error").removeClass("error");
          var data = {};
          $.each($("form :input", dialog).serializeArray(), function(i, field) {
            data[field.name] = field.value;
          });
          $("form :file", dialog).each(function() {
            data[this.name] = this.value; // file inputs need special handling
          });
          options.submit(data, function callback(errors) {
            if ($.isEmptyObject(errors)) {
              dismiss();
            } else {
              for (var name in errors) {
                showError(name, errors[name]);
              }
            }
          });
          return false;
        });
      });
    });
  }
  
  $.futonDialogs = {
    createDatabase : function() {
      $.showDialog("dialog/_create_database.html", {
        submit: function(data, callback) {
          if (!data.name || data.name.length == 0) {
            callback({name: "Please enter a name."});
            return;
          }
          $.couch.db(data.name).create({
            error: function(status, id, reason) { callback({name: reason}) },
            success: function(resp) {
              location.hash = "#/" + encodeURIComponent(data.name);
              callback();
            }
          });
        }
      });
      return false;
    }
    
    , deleteDatabase : function(dbName) {
      $.showDialog("dialog/_delete_database.html", {
        submit: function(data, callback) {
          $.couch.db(dbName).drop({
            success: function(resp) {
              callback();
              location.href = "#/";
              if (window !== null) {
                $("#dbs li").filter(function(index) {
                  return $("a", this).text() == dbName;
                }).remove();
                // $.futon.navigation.removeDatabase(dbName);
              }
            }
          });
        }
      });
    }
    
    , compactAndCleanup : function(dbName) {
      var db = $.couch.db(dbName);
      $.showDialog("dialog/_compact_cleanup.html", {
        submit: function(data, callback) {
          switch (data.action) {
            case "compact_database":
              db.compact({success: function(resp) { callback() }});
              break;
            case "compact_views":
              var groupname = page.viewName.substring(8,
                  page.viewName.indexOf("/_view"));
              db.compactView(groupname, {success: function(resp) { callback() }});
              break;
            case "view_cleanup":
              db.viewCleanup({success: function(resp) { callback() }});
              break;
          }
        }
      });
    }
  }
  
})(jQuery);

// Also add pretty JSON

(function($) {
  var _escape = function(string) {
    return string.replace(/&/g, "&amp;")
                 .replace(/</g, "&lt;")
                 .replace(/>/g, "&gt;");
  };

  // JSON pretty printing
  $.formatJSON = function (val, options) {
    options = $.extend({
      escapeStrings: true,
      indent: 4,
      linesep: "\n",
      quoteKeys: true
    }, options || {});
    var itemsep = options.linesep.length ? "," + options.linesep : ", ";

    function format(val, depth) {
      var tab = [];
      for (var i = 0; i < options.indent * depth; i++) tab.push("");
      tab = tab.join(" ");

      var type = typeof val;
      switch (type) {
        case "boolean":
        case "number":
        case "string":
          var retval = val;
          if (type == "string" && !options.escapeStrings) {
            retval = indentLines(retval.replace(/\r\n/g, "\n"), tab.substr(options.indent));
          } else {
            if (options.html) {
              retval = escape(JSON.stringify(val));
            } else {
              retval = JSON.stringify(val);
            }
          }
          if (options.html) {
            retval = "<code class='" + type + "'>" + retval + "</code>";
          }
          return retval;

        case "object": {
          if (val === null) {
            if (options.html) {
              return "<code class='null'>null</code>";
            }
            return "null";
          }
          if (val.constructor == Date) {
            return JSON.stringify(val);
          }

          var buf = [];

          if (val.constructor == Array) {
            buf.push("[");
            for (var index = 0; index < val.length; index++) {
              buf.push(index > 0 ? itemsep : options.linesep);
              buf.push(tab, format(val[index], depth + 1));
            }
            if (index >= 0) {
              buf.push(options.linesep, tab.substr(options.indent));
            }
            buf.push("]");
            if (options.html) {
              return "<code class='array'>" + buf.join("") + "</code>";
            }

          } else {
            buf.push("{");
            var index = 0;
            for (var key in val) {
              buf.push(index > 0 ? itemsep : options.linesep);
              var keyDisplay = options.quoteKeys ? JSON.stringify(key) : key;
              if (options.html) {
                if (options.quoteKeys) {
                  keyDisplay = keyDisplay.substr(1, keyDisplay.length - 2);
                }
                keyDisplay = "<code class='key'>" + _escape(keyDisplay) + "</code>";
                if (options.quoteKeys) {
                  keyDisplay = '"' + keyDisplay + '"';
                }
              }
              buf.push(tab, keyDisplay,
                ": ", format(val[key], depth + 1));
              index++;
            }
            if (index >= 0) {
              buf.push(options.linesep, tab.substr(options.indent));
            }
            buf.push("}");
            if (options.html) {
              return "<code class='object'>" + buf.join("") + "</code>";
            }
          }

          return buf.join("");
        }
      }
    }

    function indentLines(text, tab) {
      var lines = text.split("\n");
      for (var i in lines) {
        lines[i] = (i > 0 ? tab : "") + _escape(lines[i]);
      }
      return lines.join("<br>");
    }

    return format(val, 1);
  };

  // File size pretty printing
  var formatSize = function(size) {
    var jump = 512;
    if (size < jump) return size + " bytes";
    var units = ["KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"];
    var i = 0;
    while (size >= jump && i < units.length) {
      i += 1;
      size /= 1024
    }
    return size.toFixed(1) + ' ' + units[i - 1];
  }
})(jQuery);
