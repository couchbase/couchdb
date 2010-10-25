
var app = {};
window.app = app;

var isEven = function (someNumber) {
    return (someNumber%2 == 0) ? true : false;
}
var sum = function (arr) {
  var s = 0;
  for (var i=0;i<arr.length;i+=1) s += arr[i];
  return s;
}
var formatSize = function (size) {
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
var getQuery = function () {
  if (window.location.hash.indexOf('?') !== -1) {
    query = window.location.hash.slice(window.location.hash.indexOf('?')+1)
  } else {
    return;
  }
  var s = query.split('&');
  var r = {}
  for (var i=0;i<s.length;i+=1) {
    var x = s[i];
    r[x.slice(0, x.indexOf('='))] = unescape(x.slice(x.indexOf('=')+1));
  }
  return r;
}

var param = function( a ) {
  // Query param builder from jQuery, had to copy out to remove conversion of spaces to +
	var s = [];
	if ( jQuery.isArray(a) || a.jquery ) {
		jQuery.each( a, function() {
			add( this.name, this.value );
		});		
	} else {
		for ( var prefix in a ) {
			buildParams( prefix, a[prefix] );
		}
	}
  return s.join("&");

	function buildParams( prefix, obj ) {
		if ( jQuery.isArray(obj) ) {
			jQuery.each( obj, function( i, v ) {
				if (  /\[\]$/.test( prefix ) ) {
					add( prefix, v );
				} else {
					buildParams( prefix + "[" + ( typeof v === "object" || jQuery.isArray(v) ? i : "" ) + "]", v );
				}
			});				
		} else if (  obj != null && typeof obj === "object" ) {
			jQuery.each( obj, function( k, v ) {
				buildParams( prefix + "[" + k + "]", v );
			});				
		} else {
			add( prefix, obj );
		}
	}

	function add( key, value ) {
		value = jQuery.isFunction(value) ? value() : value;
		s[ s.length ] = encodeURIComponent(key) + "=" + encodeURIComponent(value);
	}
}

var request = function (options, callback) {
  options.success = function (obj) {
    callback(null, obj);
  }
  options.error = function (err) {
    if (err) callback(err);
    else callback(true);
  }
  options.dataType = 'json';
  $.ajax(options)
}
  
app.index = function () {
  $('span#topbar').html('<strong>Overview</strong>');
  $("#toolbar button.add").click($.futon.dialogs.createDatabase);
  var dbRow = function (name, even) {
    var row = $('<tr id=db-"'+name+'"><th><a href="#/'+name+'">'+name+'</a></th></tr>');
    
    row.addClass(even ? "even" : "odd")    
    row.appendTo('tbody.content');
    request({url: '/'+encodeURIComponent(name)}, function ( err, info ) {
      if (err) info = { disk_size:"can't connect", doc_count:"can't connect"
                      , update_seq:"can't connect"};
      row.append('<td class="size">'+formatSize(info.disk_size)+'</td>' +
                  '<td class="count">'+info.doc_count+'</td>' + 
                  '<td class="seq">'+info.update_seq+'</td>'
                  );
    });
  }
  
  request({url: '/_all_dbs'}, function (err, dbs) {
    if (err) $('tbody.content').append('<tr><td>error</td><td>error</td><td>error</td></tr>');
    else {
      var moreRows = function (start, limit) {
        for (var i=start;i<(start + limit);i+=1) { 
           if (dbs[i]) dbRow(dbs[i], isEven(i));
           else {$('div#pagination').remove(); return;}
        }
        $('span.more').unbind('click');
        $('span.more').click(function ( ) { moreRows(i, parseInt($('#pages-input').val())) })
      }
      if (dbs.length > 20) {
        var pagination = '<div id="pagination"><span class="more">Load </span><input type="text" id="pages-input" value=20></input><span class="more"> More Items</span></div>'
         $('td.more').append(pagination);
      }
      moreRows(0, 20);
    }
  })

}
app.showDatabase = function () {
  var db = this.params['db']
    , query = getQuery()
    ;
  
  var init = function () {
    $('span#topbar').html('<a href="#/">Overview</a><strong>'+db+'</strong>');
    $("#toolbar button.add").click( function () { 
      $("div#content").html('');
      request({url:'/_uuids'}, function (err, resp) {
        location.hash = '#/' + db + '/' + resp.uuids[0]
      })
      // location.hash = "#/" + db + '/_new';
    });
    $("#toolbar button.compact").click(function () { 
      $.futon.dialogs.compactAndCleanup(db)
    });
    $("#toolbar button.delete").click(function (){$.futon.dialogs.deleteDatabase(db)});
    // $("#toolbar button.security").click(page.databaseSecurity); TODO : New security UI
    
    // JumpToDoc
    $('input#jumptodoc').change(function () {
      window.location.hash = '#/' + db + '/' + $(this).val();
    })
    
    var addquery = function () {
      // This function adds the _all_docs startkey/endkey query options
      $('select.dbquery-select').before(
        '<div class="alldoc-query">' + 
          '<span class="query-option">end<input class="query-option" id="end" type="text"></input></span>' +
          '<span class="query-option">start<input class="query-option" id="start" type="text"></input></span>' +
        '</div>'
      );
      $('input.query-option').change(function () {
        var startkey = $('input#start').val()
          , endkey = $('input#end').val()
          ;
        // Check if the keys are properly json encoded as strings, if not do it 
        if (startkey[0] !== '"' && startkey.length !== 0) startkey = '"'+startkey+'"'
        if (endkey[0] !== '"' && endkey.length !== 0) endkey = '"'+endkey+'"'
        // Craft query
        h = '#/'+db+'/_all_docs?';
        if (startkey.length > 0) h += ('startkey='+escape(startkey) + '&');
        if (endkey.length > 0) h += ( 'endkey='+escape(endkey) + '&');
        window.location.hash = h;
      });
    }

    request({url: '/'+encodeURIComponent(db)}, function (err, info) {
      // Fill out all info from the db query.
      for (i in info) {$('div#'+i).text(info[i])}
      var disk_size = info.disk_size;
      $('div#disk_size').text(formatSize(info.disk_size))
      
      // Query for ddocs to calculate size
      request({url:'/'+encodeURIComponent(db)+'/_all_docs?startkey="_design/"&endkey="_design0"'}, function (err, docs) {
        var sizes = [];
        for (var i=0;i<docs.rows.length;i+=1) {
          // Query every db for it's size info
          // Note: because of a current bug this query sometimes causes a view update even with stale=ok
          request({url:'/'+encodeURIComponent(db)+'/'+docs.rows[i].id+'/_info?stale=ok'}, function (err, info) {
            if (err) throw err
            sizes.push(info.view_index.disk_size);
            if (sizes.length === docs.rows.length) {
              // All queries are finished, update size info
              var s = sum(sizes)
              $('div#views_size').text(formatSize(s));
              $('div#full_size').text(formatSize(s + disk_size));
            }
          })
        }
        if (docs.rows.length === 0) {
          // There are no design documents, db size is full size
          $('div#views_size').text(formatSize(0));
          $('div#full_size').text(formatSize(disk_size));
        }
      })
    })

    $('select.dbquery-select').change(function (e) {
      if (e.target.value === 'all') {
        // All Documents selected, bounce back to dburl
        $('div.alldoc-query').remove();
        if (query) {window.location.hash = '#/'+db}
      } else if (e.target.value === 'ddocs') {
        // Design doc was selected, pop out query and fill with ddoc query
        $('div.alldoc-query').remove();
        addquery();
        $('input#start').val('_design/');
        $('input#end').val('_design0').change();
      } else if (e.target.value === 'query') {
        // Query selected, pop out query options
        $('div.alldoc-query').remove();
        addquery();
      }
    })
    if (query) {
      if (query.startkey || query.endkey) {
        // There is an open all docs query, pop out query options and fill in with current query
        $('option[value=all]').attr('selected', false);
        $('option[value=query]').attr('selected', true).change();
        $('input#start').val(query.startkey ? query.startkey : '');
        $('input#end').val(query.endkey ? query.endkey : '');
      }
    }
  } 
  
  var rowCount = 0;
  var moreRows = function (start, limit) {
    // This function adds more rows to the current document table
    if (query) {
      query.limit = limit
      query.skip = start
    } else {
      query = {limit:limit, skip:start}
    }
    request({url: '/'+encodeURIComponent(db)+'/_all_docs?'+param(query)}, function (err, resp) {
      if (err) throw err;
      for (var i=0;i<resp.rows.length;i+=1) {
        row = $('<tr><td><a href="#/'+db+'/'+resp.rows[i].key+'">'+resp.rows[i].key+'</a></td><td>' +
                 resp.rows[i].value.rev+'</td></tr>'
               )
               // rowCount currently breaks on odd pagination values
               .addClass(isEven(rowCount) ? "even" : "odd")
               .appendTo('tbody.content')
               ;
        rowCount += 1;
     }
     if (!$('span.more').length && (resp.rows.length == limit) ) {
       // The number of rows is less than the limit and we haven't added the pagination element yet 
       $('td.more').append('<div id="pagination"><span class="more">Load </span><input type="text" id="pages-input" value='+limit+'></input><span class="more"> More Items</span></div>');
     } else if ( resp.rows.length < limit ) {
       // If the return rows are less than the limit we can remove pagination
       $('div#pagination').remove()
     }
     // Remove the previous pagination handler and add a new one with the new closure values
     $('span.more').unbind('click');
     $('span.more').click(function ( ) { moreRows(start + limit, parseInt($('#pages-input').val())) });
   })
  }
  
  // Decide whether or not to load the template content
  if ( $('table#documents').length === 0) {    
    this.render('templates/database.mustache', {db:db})
      .replace('#content')
      .then(function () {init(); moreRows(0,20);})
      
  } else {
    // If the template content is already there, remove the current content
    $('tr.even').remove();
    $('tr.odd').remove();
    moreRows(0, 20);
  }
}

function getType (obj) {
  if (obj === null) return 'null'
  if (typeof obj === 'object') {
    if (obj.constructor.toString().indexOf("Array") !== -1) return 'array'
    else return 'object'
  } else {return typeof obj}
}

function largestWidth (selector) {
  var min_width = 0;
  $(selector).each(function(i, n){
      var this_width = $(n).width();
      if (this_width > min_width) {
          min_width = this_width;
      }
  });
  return min_width;
}

$.expr[":"].exactly = function(obj, index, meta, stack){ 
  return ($(obj).text() == meta[3])
}

function coerceFieldValue (val) {
  if (val == 'null') return null;
  
  if (val.indexOf('.') !== -1) {
    if (!isNaN(parseFloat(val))) return parseFloat(val)
  } else {
    if (!isNaN(parseInt(val))) return parseInt(val)
  }
  
  if (val[0] === '"' && val[val.length - 1] === '"') {
    return val.slice(1, val.length -1);
  }
  if (val[0] === '[' && val[val.length - 1] === ']') {
    return JSON.parse(val);
  }
  if (val[0] === '{' && val[val.length - 1] === '}') {
    return JSON.parse(val);
  } else {
    return val;
  }
}

app.showDocument = function () {
  var db = this.params['db']
    , docid = this.params['docid']
    , _doc
    , url = '/' + db + '/' + docid
    ;
  $('span#topbar').html('<a href="#/">Overview</a><a href="#/'+db+'">'+db+'</a><strong>'+docid+'</strong>'); 
  
  var getEdit = function (obj, key, val, minWidth) {
    var edit = function () {
      $(this).find('span.expand').click();
      var w = $(this).width();
      if (w < minWidth) {
        w = minWidth;
      }
      var h = $(this).height();
      val.html('')
      val.html(
        $('<textarea type="text" />')
        .val(JSON.stringify(obj[key]))
        .width(w)
        .height(h)
        .change(function () {
          obj[key] = coerceFieldValue($(this).val());
          request({url:url, type:'PUT', data:JSON.stringify(_doc), processData:false}, function (err, newresp) {
            if (err) console.log(err)
            _doc._rev = newresp.rev;
            $("div.doc-key:exactly('_rev')").next().html(createValue.string(_doc, '_rev', false));
            val.parent().append(createValue[getType(obj[key])](obj, key));
            val.remove();
          })
        })
      )
    }
    return edit;
  }
  
  var createValue = {
    "string": function (obj, key, editable) {
      var val = $('<div class="doc-value string-type"></div>')
        , edit = getEdit(obj, key, val, 350)
        ;
      if (obj[key].length > 45) {
        val.append($('<span class="string-type"></span>')
        .click(editable ? edit : function () {  })
        .text(obj[key].slice(0, 45)))
        .append(
          $('<span class="expand">...</span>')
          .click(function () {
            val.html('')
            .append($('<span class="string-type"></span>')
            .click(editable ? edit : function () {  })
            .text(obj[key]))
          })
        )
      }
      else {
        var val = $('<div class="doc-value string-type"></div>')
          , edit = getEdit(obj, key, val, 350)
          ;
        val.append(
          $('<span class="string-type"></span>')
          .click(editable ? edit : function () {  })
          .text(obj[key])
        )
      }
      return val;
    }
    , "number": function (obj, key, editable) {
      var val = $('<div class="doc-value number"></div>')
      val.append($('<span class="number-type">' + obj[key] + '</span>').click(getEdit(obj, key, val, 100)))
      return val;
    }
    , "null": function (obj, key, editable) {
      var val = $('<div class="doc-value null"></div>')
      val.append($('<span class="null-type">' + obj[key] + '</span>').click(getEdit(obj, key, val,100)))
      return val;
    }
    , "boolean": function (obj, key, editable) {
      var val = $('<div class="doc-value null"></div>')
      val.append($('<span class="null-type">' + obj[key] + '</span>').click(getEdit(obj, key, val,100)))
      return val;
    }
    , "array": function (obj, key, editable, _attachments, indent) {
       if (!indent) indent = 1;
        var val = $('<div class="doc-value array"></div>')
        $('<span class="array-type">[</span><span class="expand" style="float:left">...</span><span class="array-type">]</span>')
          .click(function (i, n) {
            var n = $(this).parent();
            var cls = 'sub-'+key+'-'+indent
            n.html('')
            n.append('<span style="padding-left:'+((indent - 1) * 10)+'px" class="array-type">[</span>')
            for (i in obj[key]) {

              n.append(
                $('<div class="doc-field">' +
                    '<div class="array-key '+cls+'" style="padding-left:'+(indent * 10)+'px">'+i+'</div>' +
                  '</div>'
                  )
                  .append(createValue[getType(obj[key][i])](obj[key], i, true, false, indent + 1))
                )
            }
            n.append('<span style="padding-left:'+((indent - 1) * 10)+'px" class="array-type">]</span>')
            $('div.'+cls).width(largestWidth('div.'+cls))
          })
          .appendTo($('<div class="array-type"></div>').appendTo(val))
        return val;
    }
    , "object": function (obj, key, editable, _attachments, indent) {
      if (!indent) indent = 1;
      var val = $('<div class="doc-value object"></div>')
      $('<span class="object-type">{</span><span class="expand" style="float:left">...</span><span class="object-type">}</span>')
        .click(function (i, n) {
          var n = $(this).parent();
          var cls = hex_sha1('sub-'+key+'-'+indent)
          n.html('')
          n.append('<span style="padding-left:'+((indent - 1) * 10)+'px" class="object-type">{</span>')
          for (i in obj[key]) {

            var field = $('<div class="doc-field"></div>')
            
            if (!_attachments) {
              if (editable !== false) {
                var p = $('<div class="delete-button" style="margin-left:'+(indent * 10)+'px"/>').click((function (field, i) {
                    return function () {
                      delete obj[key][i]
                      request({url:url, type:'PUT', data:JSON.stringify(_doc), processData:false}, function (err, newresp) {
                        if (err) console.log(err);
                        else {
                          _doc._rev = newresp.rev;
                          $("div.doc-key:exactly('_rev')").next().html(createValue.string(_doc, '_rev', false));
                          field.remove();
                        }
                      })
                    }
                  })(field, i))

              } else {
                var p = $('<div class="id-spacer" style="margin-left:'+(indent * 10)+'px"/>')
              }
              field.append(p)
                .append('<div class="object-key '+cls+'" >'+i+'</div>')
                .append(createValue[getType(obj[key][i])](obj[key], i, editable, false, indent + 1))
              n.append(field)
            } else {
              field.append(
                $('<div class="delete-button" style="margin-left:'+(indent * 10)+'px"/>')
                .click((function (field, i) {
                  return function () {
                    delete obj[key][i]
                    request({url:url, type:'PUT', data:JSON.stringify(_doc), processData:false}, function (err, newresp) {
                      if (err) console.log(err);
                      else {
                        _doc._rev = newresp.rev;
                        $("div.doc-key:exactly('_rev')").next().html(createValue.string(_doc, '_rev', false));
                        field.remove();
                      }
                    })
                  }
                })(field, i))
                )
                .append('<div class="object-key '+cls+'" ><a href="'+url+'/'+i+'">'+i+'</a></div>')
                .append(createValue[getType(obj[key][i])](obj[key], i, false, false, indent + 1))
              n.append(field)
            }
            
          }
          n.append('<span style="padding-left:'+((indent - 1) * 10)+'px" class="object-type">}</span>')
          $('div.'+cls).width(largestWidth('div.'+cls))
        })
        .appendTo($('<div class="object-type"></div>').appendTo(val))
      return val;
    }
  }
  
  this.render('templates/document.mustache', {db:db,docid:docid}).replace('#content').then(function () {
    request({url:url}, function (err, resp) {
      var setRev = false;
      _doc = resp;
      
      if (err) console.log(err)
      else {
        var d = $('div#document-editor');
        for (i in resp) {
          var field = $('<div class="doc-field"></div>')
          if (i !== '_rev' && i !== '_id') {
            $('<div class="delete-button" />').click((function (field, i) {
              return function () {
                delete _doc[i]
                request({url:url, type:'PUT', data:JSON.stringify(_doc), processData:false}, function (err, newresp) {
                  if (err) console.log(err);
                  else {
                    _doc._rev = newresp.rev;
                    $("div.doc-key:exactly('_rev')").next().html(createValue.string(_doc, '_rev', false));
                    field.remove();
                  }
                })
              }
            })(field, i))
            .appendTo(field)
          } else {
            $('<div class="id-space" />').appendTo(field);
          }
          
          field.append('<div class="doc-key doc-key-base">'+i+'</div>')
          field.append(createValue[getType(resp[i])](resp, i, (i !== '_rev' && i !== '_id') , 
            (i == '_attachments'))
          )
          d.append(field)
        }
      }

      $('div.doc-key-base').width(largestWidth('div.doc-key-base'))
    })  
  })
}

app.showChanges = function () {
  var db = this.params['db'];
  $('span#topbar').html('<a href="#/">Overview</a><a href="#/'+db+'">'+db+'</a><strong>_changes</strong>');  
  this.render('templates/changes.mustache').replace('#content').then(function () {
    var query = getQuery()
      , url = '/'+db+'/_changes'
    if (query) url += ('?' + param(query));
    var rowCount = 0;
    request({url:url}, function (err, resp) {
      // Render the response in 10 row chunks for efficiency
      var pending = []
        , renderPending = function () {$('table#changes').append(pending.join('')); pending = [];}
        , c , changes, baseUrl = '#/'+db+'/'
        ;
      for (var i=0;i<resp.results.length;i+=1) {
        c = resp.results[i]; 
        changes = []
        $.each(c.changes, function (x, change) {
          changes.push(change.rev);
        })
        pending.push('<tr class="'+(isEven(rowCount) ? "even" : "odd")+'">' + 
                       '<td><a href="'+baseUrl+'_changes?since='+(c.seq-1)+'">'+c.seq+'</td>'+
                       '<td><a href="'+baseUrl+c.id+'">'+c.id+'</a><td>['+changes.join(', ')+']</td></tr>'
                    )
        if (pending.length > 10) renderPending();
        rowCount += 1;
      }
      renderPending()
    })
  })
}

app.showConfig = function () {
  $('span#topbar').html('<strong>Configuration</strong>');
  this.render('templates/config.mustache').replace('#content').then(function () {
    
  })
}

app.showStats = function () {
  $('span#topbar').html('<strong>Status</strong>');
  this.render('templates/stats.mustache').replace('#content').then(function () {
    request({url:'/_stats'}, function (err, stats) {
      var info = $('#content').append('<h2>Raw Info</h2>')
        , text = ''
        ;
      for (i in stats) {
        text += '<div class="stat-section">'+i+'</div>'
        for (x in stats[i]) {
          text += '<div class="stat-subsection">'+x+'<span class="stat-subsection-description">'+stats[i][x].description+'</span></div>'
          for (y in stats[i][x]) {
            if (y !== 'description') {
              text += '<span class="stat-title">'+y+'</span>'
              text += '<span class="stat-value"> ' + 
                ((stats[i][x][y] === null) ? 'none' : stats[i][x][y]) + 
                ' </span>'
            }
          }
          text += '<br>'
        }
        text += '<br>'
        info.append(text)
        text = ''
      }
    })
  })
}

app.showTests = function () {
  $('span#topbar').html('<strong>Test Suite</strong>');
  this.render('templates/tests.mustache').replace('#content').then(function () {
    
  })
}
app.showReplicator = function () {
  $('span#topbar').html('<strong>Replicator</strong>');
  this.render('templates/replicator.mustache').replace('#content').then(function () {
    
  })
}

app.showView = function () {
  var db = this.params['db']
    , ddoc = this.params['ddoc']
    , view = this.params['view']
    ;
  
  var populateViews = function (ddoc, view) {
    var v = $('select#view-select');
    app.ddoc_ = ddoc;
    v.css('color', '#1A1A1A')
    v.attr('loaded', true)
    if (!ddoc.views) {
      v.append($('<option value="No Views">No Views</option>'))
    } else {
      v.html('')
      v.attr('disabled', false);
      if (!view) { v.append('<option value="Select View">Select View</option>') }
      for (i in ddoc.views) { 
        if (view && i === view) {
          v.append('<option value="'+i+'" selected>'+i+'</option>'); 
        } else {
          v.append('<option value="'+i+'">'+i+'</option>'); 
        }
      }
      v.change(function () {
        refresh();
      })
    }
  }
  
  var refresh = function () {
    var h = '#/' + db + '/' + $('select#ddoc-select').val() + '/_view/' + $('select#view-select').val()
      , query = {}
      ;
    $('input.qinput').each(function (i, n) {
      n = $(n)
      var name = n.attr('name')
        , type = n.attr('type')
        , val = n.val()
        ;
      if (type == "text") {
        if (val.length > 0) {
          if (name == "skip" || name == "limit" || name == "group_level") {
            query[name] = parseInt(n.val())
          } else if (name == "startkey_docid" || name == "endkey_docid" ) {
            if (val[0] == '"') query[name] = val
            else query[name] = JSON.stringify(val)
          } else if (name == "startkey" || name == "endkey" || name == "key") {
            if (val[0] == '"' || val[0] == '[' || val[0] == '{') query[name] = val
            else query[name] = JSON.stringify(val)
          }
        }
      } else if (type == "checkbox" && n.attr("checked")) {
        if (name == "stale") query[name] = 'ok'
        else query[name] = 'true'
      }
    })
    
    window.location.hash = h + '?' + param(query);
  }
  
  var setupViews = function () {
    if (!$('select#ddoc-select').attr('loaded')) {
      request({url: '/' + encodeURIComponent(db) + 
                    '/_all_docs?startkey="_design/"&endkey="_design0"&include_docs=true'}, 
                    function (err, docs) { 
        var s = $('select#ddoc-select');
        s.attr('loaded', true)
        docs.rows.forEach(function (row) {
          if (ddoc) {
            s.append($('<option value="'+row.id+'" selected>'+row.id+'</option>'))
          } else {
            s.append($('<option value="'+row.id+'">'+row.id+'</option>'))
          }
        })
        s.change(function () {
          request({url: '/'+ encodeURIComponent(db) + '/' + s.val()}, function (err, ddoc) {
            populateViews(ddoc)
          })
        })
      })
    } 
    
    
    
    var updateResults = function () {
      var c = $('tbody.content')
        , url = window.location.hash.replace('#','')
        ;
        
      c.html('');
      request({url:url}, function (err, resp) {
        $('td#viewfoot').html('')
        if (!resp) {
          err = JSON.parse(err.responseText);
          $('td#viewfoot').append($(
            '<div class="error-type">Error : ' + err.error + '</div>' + 
            '<div class="error-reason">Reason : '+ err.reason + '</div>'
          ))
        } else {
          $('th.doc').remove()
          if (getQuery().include_docs) {
            $('tr.viewhead').append('<th class="doc">doc<span class="expand-all">⟱</span></th>').find('span')
              .click(function () {$('span.expand-doc').click()})
          }
          var odd = 'even';
          resp.rows.forEach(function (row) {
            var tr = $('<tr class="' + odd + '">' + 
                         '<td class="key">' + 
                           '<div class="viewkey">' + 
                             '<span><strong>'+JSON.stringify(row.key)+'</strong></span>' +
                             '<span class="viewkey">^</span>' +
                             '<span class="viewend">\></span>' + 
                             '<span class="viewstart">\<</span>' + 
                           '</div>' + 
                           '<div class="docid">' + 
                             '<span class="docid">ID: ' + row.id + '</span>' + 
                             '<span class="viewend">\></span>' + 
                             '<span class="viewstart">\<</span>' +
                           '</div>' +
                         '</td>' +
                         '<td class="value"><code>' + row.value + '</code></td>' + 
                       '</tr>')
            if (row.doc) {
              var expand = function () {
                var d = $('<tr class="showdoc"><td class="showdoc" colspan="4"><code>'  +
                    $.futon.formatJSON(row.doc) + '</code></td></tr>'
                  )
                var collapse = function () {
                  d.remove();
                  $(this).text('⟱').css('cursor', 'pointer').unbind('click', collapse);
                  $(this).click(expand)
                }
                $(this).text('⟰').css('cursor', 'pointer').unbind('click', expand);
                $(this).click(collapse)
                d.insertAfter(tr)
              }
              
              $('<td>' + '<span class="expand-doc">⟱</span>' + '</td>')
                .click(expand)
                .appendTo(tr)
                ;
            }
            if (odd == 'odd') odd = 'even'
            else odd = 'odd'
            c.append(tr)
          })
          
          // Add quick links for setting key, startkey, endkey, startkey_docid & endkey_docid
          $("span.viewstart").click(function () {
            var c = $(this).parent();
            if (c.attr('class') == 'viewkey') {
              $("input[name='startkey']").val(c.text().slice(0, c.text().length - 3)).change();
            } else if (c.attr('class') == 'docid') {
              $("input[name='startkey_docid']").val(c.text().slice(4, c.text().length - 2)).change();
            }
          })
          $("span.viewend").click(function () {
            var c = $(this).parent();
            if (c.attr('class') == 'viewkey') {
              $("input[name='endkey']").val(c.text().slice(0, c.text().length - 3)).change();
            } else if (c.attr('class') == 'docid') {
              $("input[name='endkey_docid']").val(c.text().slice(4, c.text().length - 2)).change();
            }
          })
          $("span.viewkey").click(function () {
            var c = $(this).parent();
            $("input[name='key']").val(c.text().slice(0, c.text().length - 3)).change();
          })
          
          // Add view result info
          $('td#viewfoot').append(
            '<div class="viewinfo" >total_rows<span class="viewinfo-val">' + 
              resp.total_rows + '</span></div>'+
            '<div class="viewinfo" >offset<span class="viewinfo-val">' + 
              resp.offset + '</span></div>' +
            '<div class="viewinfo" >rows<span class="viewinfo-val">' + 
              resp.rows.length + '</span></div>'
          )
        }
      })
    }
    
    var release = function () {
      // Clear all fields
      $("input.qinput[type='text']").val('');
      $("input.qinput[type='checkbox']").attr('checked', false);
      // Repopulate all the fields from the url
      var query = getQuery();
      for (i in query) {
        var n = $('input[name='+i+']')
          , type = n.attr("type")
          ;
        if (type == "text") n.val(query[i])          
        else if (type == 'checkbox' && (query[i] == 'true' || query[i] == 'ok')) n.attr('checked', 'true')
      }
      
      if (!$('input.quinput[name=limit]').attr('released')) {
        $('*.qinput').css('color', '#1A1A1A');
        $('*.qinput').attr('disabled', false);
        if (!app.ddoc_.views[view].reduce) {
          $('input.reduce').attr('disabled', true)
          $('span.reduce').css('color', '#A1A1A1');
        }
        
        $("input.qinput[type='checkbox']").click(refresh);
        $("input[type='text']").change(refresh);
        $("input.qinput[name='limit']").attr('released', true)
      }
      
      // refresh();
      updateResults();
    }
    
    if (!$('select#view-select').attr('loaded') && ddoc) {
      request({url: '/'+ encodeURIComponent(db) + '/_design/' + ddoc}, function (err, ddoc) {
        populateViews(ddoc, view);
        release();
      })
    } else if (ddoc) { release(); }
    
  }
  
  $('span#topbar').html('<a href="#/">Overview</a><a href="#/'+db+'">'+db+'</a><strong>_view</strong>');
  if ($('div#query-options').length === 0) {
    this.render('templates/view.mustache').replace('#content').then(setupViews);
  } else {setupViews();}
}

app.wildcard = function () {
  var args = this.path.split('/');
  args.splice(0,1);
  this.params.db = args.splice(0,1);
  this.params.docid = args.join('/')
  app.showDocument.call(this, arguments)
}

var a = $.sammy(function () {
  
  var indexRoute = function () {
    this.render('templates/index.mustache').replace('#content').then(app.index);
  }
  // Index of all databases
  this.get('', indexRoute);
  this.get("#/", indexRoute);
  
  this.get('#/_config', app.showConfig);
  this.get('#/_stats', app.showStats);
  this.get('#/_tests', app.showTests);
  this.get('#/_replicate', app.showReplicator)
  
  this.get('#/:db/_views', app.showView);
  this.get('#/:db/_design/:ddoc/_view/:view', app.showView);
  
  // Database view
  this.get('#/:db', app.showDatabase);
  this.get('#/:db/_all_docs', app.showDatabase);
  // Database _changes feed
  this.get('#/:db/_changes', app.showChanges);
  
  // Database views viewer
  this.get('#/:db/_views', app.showViews);
  // Document editor/viewer
  this.get('#/:db/:docid', app.showDocument);
  
  this.get(/\#\/(*)/, app.wildcard)
})

$(function () {
  $("span#raw-link").click(function () {
    window.location = window.location.hash.replace('#','');
  })
  
  a.use('Mustache'); 
  a.run(); 
  
});