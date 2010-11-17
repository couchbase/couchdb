  
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

function getType (obj) {
  if (obj === null) return 'null'
  if (typeof obj === 'object') {
    if (obj.constructor.toString().indexOf("Array") !== -1) return 'array'
    else return 'object'
  } else {return typeof obj}
}

function largestWidth (selector, min) {
  var min_width = min || 0;
  $(selector).each(function(i, n){
      var this_width = $(n).width();
      if (this_width > min_width) {
          min_width = this_width;
      }
  });
  return min_width;
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
  if (options.data && typeof options.data == 'object') {
    options.data = JSON.stringify(options.data)
  }
  options.processData = false;
  options.dataType = 'json';
  options.contentType = 'application/json'
  $.ajax(options)
}

var handleError = function (err, resp) {
  if (!resp) resp = err.responseText
  try {resp = JSON.parse(resp)}
  catch(e) {}
  var e = $('<div class="error-bubble"></div>')
  if (err.status) e.append('<span class="error-code">'+err.status+'</span>')
  
  e.append('<span class="error-title">'+resp.error || err.statusText || resp+'</span>')
  if (resp.error) e.append('<br>').append('<span class="error-text">'+resp.reason+'</span>')
  
  // Because of futon's crazy scroll constraints we can't leave the error
  // container in the default html and have to append it to content when it's not there
  
  if (!$('div#error-container').length) {
    $('div#content').prepend('<div id="error-container"></div>')
  }
  e.appendTo('div#error-container')
  var r = $('<span class="remove-error"></span>')
  .click(function () {
    $(this).remove();
    e.remove();
  })
  e.parent().append(r);
  var p = e.position();
  r.css({left:p.left+e.outerWidth()+5, top:p.top + (e.outerHeight() / 2) - (r.outerHeight()/2)})
  if (console) console.log(err)
  throw {err:err, resp:resp, e:e}
}

$.expr[":"].exactly = function(obj, index, meta, stack){ 
  return ($(obj).text() == meta[3])
}

app.showIndex = function () {
  var t = this
    , a = arguments
    ;
  this.render('templates/index.mustache').replace('#content').then(function () {
    app.loadIndex.apply(t, a)
  });
}

app.showDocument = function () {
  var t = this
    , a = arguments
    ;
  this.render('templates/document.mustache', this.params).replace('#content').then(function () {
    app.loadDocument.apply(t, a);
  })
}

app.showChanges = function () {
  var db = this.params['db']
    , t = this
    , a = arguments
    ;
  $('span#topbar').html('<a href="#/">Overview</a><a href="#/'+encodeURIComponent(db)+'">'+db+'</a><strong>_changes</strong>');  
  this.render('templates/changes.mustache').replace('#content').then(function () {
    app.loadChanges.apply(t, a)
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
var ddoc_;

app.showView = function () {
  var db = this.params['db']
    , ddoc = this.params['ddoc']
    , view = this.params['view']
    , _this = this
    , _args = arguments
    ;
  
  var refresh = function () {
    var h = '#/' + encodeURIComponent(db) + '/_design/' + ddoc + '/_view/' + view
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
          if (getQuery() && getQuery().include_docs) {
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
                         '<td class="value"><code>' + $.formatJSON(row.value) + '</code></td>' + 
                       '</tr>')
            if (row.doc) {
              var expand = function () {
                var d = $('<tr class="showdoc"><td class="showdoc" colspan="4"><code>'  +
                    $.formatJSON(row.doc) + '</code></td></tr>'
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
        
        if (!ddoc_.views[view] && !ddoc_.views[view].reduce) {
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
    
    if (!$("div.view-ddoc").length) {
      // No views in the list, populat
      request({url: '/' + encodeURIComponent(db) + 
                    '/_all_docs?startkey="_design/"&endkey="_design0"&include_docs=true'}, 
                    function (err, docs) {
        if (err) handleError(err, docs);
        $("div#view-selection").attr('loaded', true); 
        var s = $('div#ddoc-selection');
        var getAddView = function () {
          var addView = $('<div class="ddoc-view-select"><span class="add-view">new</span></div>')
          addView.click(function () {
            var self = $(this)
            $('<input class="new-view-field"></input>')
            .change(function () {
              view = $(this).val();
              $("span.add-view").parent().before('<div class="ddoc-view-select">'+view+'</div>');
              $('div#view-editor').show();
              $('textarea#view-editor-reduce').val('')
              $('textarea#view-editor-map').val('').focus();
              $(this).remove();
              self.show();
            })
            .appendTo(self.parent())
            .focus()
            ;
            self.hide();
          })
          return addView;  
        }
        
        docs.rows.forEach(function (row) {
          var populate = function () {
            var v = $("div#ddoc-view-selection");
            v.html('')
            if (row.doc.views) {
              for (viewName in row.doc.views) {
                $('<div class="ddoc-view-select">'+viewName+'<span class="edit-view">edit</span></div>')
                .appendTo(v)
                .click(function () {
                  window.location.hash = "#/"+encodeURIComponent(db)+'/'+row.id+'/_view/'+encodeURIComponent(viewName)+'?limit=10'
                })
              }
            }
            v.append(getAddView());
            $("span.edit-view")
            .click(function () {
              var v = $(this).parent().text()
              v = v.slice(0, v.length -4)
              $('div#view-editor').show();
              $('textarea#view-editor-map').val(ddoc_.views[v].map)
              $('textarea#view-editor-reduce').val(ddoc_.views[v].reduce)
            })
            ddoc_ = row.doc;
            if (view) {
              release();
            }             
          }
          
          $('<div class="view-ddoc">'+row.id+'</div>')
          .click(function () {
            populate();
            $("div.view-ddoc-selected").removeClass("view-ddoc-selected");
            $(this).addClass("view-ddoc-selected");
            $("*.qinput").attr('disabled', true).css('color', '#A1A1A1');
            $("tbody.content").html('')
            $('td#viewfoot').html('')
            $('div#view-editor').hide();
            window.location.hash = "#/"+encodeURIComponent(db)+'/'+row.id+'/_view/'
          })
          .appendTo(s)
          if ('_design/'+ddoc == row.id) {
            populate();
          } 
        })
        $('<div class="view-ddoc"><span class="add-ddoc">new</span></div>')
        .click(function () {
          $('<input class="new-view-field"></input>')
          .appendTo($(this).parent())
          .change(function () {
            var id = $(this).val();
            if (id.slice(0, '_design/'.length) !== '_design/') id = '_design/'+id
            
            $(this).parent().append('<div class="view-ddoc">'+id+'</div>')
            $(this).remove();
            ddoc = id.replace('_design/', '')
            ddoc_ = {_id:id, views:{}}
            var av = getAddView();
            $("div#ddoc-view-selection")
            .html('')
            .append(av)
            ;
            av.click();
          })
          .focus()
          ;
          $(this).remove()
        })
        .appendTo(s)
        ;
      })
      $('span.save-view-button')
      .unbind('click')
      .click(function () {
        var m = $('textarea#view-editor-map').val()
          , r = $('textarea#view-editor-reduce').val()
          ;
        if (!ddoc_.views) ddoc_.views = {}
        ddoc_.views[view] = {}
        if (m.length) ddoc_.views[view].map = m;
        if (r.length) ddoc_.views[view].reduce = r;
        request({url:'/'+encodeURIComponent(db), type:'POST', data:ddoc_}, function (err, resp) {
          if (err) handleError(err, resp);
          $('div#content').html('');
          var oldHash = window.location.hash
            , h = '#/' + encodeURIComponent(db) + '/_design/' + ddoc + '/_view/' + view + "?limit=10"
            ;
          if (oldHash !== h) window.location.hash = h;
          else {app.showView.apply(_this, _args)}
        })
      })
      ;
      
    } else if (view) {release()}
    
  }
  
  $('span#topbar').html('<a href="#/">Overview</a><a href="#/'+encodeURIComponent(db)+'">'+db+'</a><strong>_view</strong>');
  if ($('div#query-options').length === 0) {
    this.render('templates/view.mustache').replace('#content').then(setupViews);
  } else {setupViews();}
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
        if (err) handleError(err, resp);
        location.hash = '#/' + db + '/' + resp.uuids[0]
      })
      // location.hash = "#/" + db + '/_new';
    });
    $("#toolbar button.compact").click(function () { 
      $.futonDialogs.compactAndCleanup(db)
    });
    
    $("#toolbar button.delete").click(function (){$.futonDialogs.deleteDatabase(db)});
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
      if (err) handleError(err, info);
      // Fill out all info from the db query.
      for (i in info) {$('div#'+i).text(info[i])}
      var disk_size = info.disk_size;
      $('div#disk_size').text(formatSize(info.disk_size))
      
      // Query for ddocs to calculate size
      request({url:'/'+encodeURIComponent(db)+'/_all_docs?startkey="_design/"&endkey="_design0"'}, function (err, docs) {
        if (err) handleError(err, docs)
        var sizes = [];
        for (var i=0;i<docs.rows.length;i+=1) {
          // Query every db for it's size info
          // Note: because of a current bug this query sometimes causes a view update even with stale=ok
          request({url:'/'+encodeURIComponent(db)+'/'+docs.rows[i].id+'/_info?stale=ok'}, function (err, info) {
            if (err) handleError(err, info);
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
      if (err) handleError(err, resp);
      for (var i=0;i<resp.rows.length;i+=1) {
        row = $('<tr><td><a href="#/'+db+'/'+encodeURIComponent(resp.rows[i].key)+'">'+resp.rows[i].key+'</a></td><td>' +
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
  if ( $('div#dbinfo').length === 0) {    
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

app.wildcard = function () {
  var args = this.path.split('/');
  args.splice(0,1);
  this.params.db = args.splice(0,1);
  this.params.docid = args.join('/')
  app.showDocument.call(this, arguments)
}

var a = $.sammy(function () {
  // Index of all databases
  this.get('', app.showIndex);
  this.get("#/", app.showIndex);
  
  this.get('#/_config', app.showConfig);
  this.get('#/_stats', app.showStats);
  this.get('#/_tests', app.showTests);
  this.get('#/_replicate', app.showReplicator)
  
  this.get('#/:db/_views', app.showView);
  this.get('#/:db/_design/:ddoc/_view/', app.showView);
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
  
  this.get(/\#\/(.*)/, app.wildcard)
})

$(function () {
  $("span#raw-link").click(function () {
    window.location = window.location.hash.replace('#','');
  })
  
  a.use('Mustache'); 
  a.run(); 
  
});
