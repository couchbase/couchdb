
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
var parseQuery = function (query) {
  if (query[0] === '?') query = query.slice(1);
  var s = query.split('&');
  var r = {}
  for (var i=0;i<s.length;i+=1) {
    var x = s[i];
    r[x.slice(0, x.indexOf('='))] = unescape(x.slice(x.indexOf('=')+1));
  }
  return r;
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
  $('h1#topbar').html('<strong>Overview</strong>');
  var dbRow = function (name, even) {
    var row = $('<tr id=db-"'+name+'"><th><a href="#/'+name+'">'+name+'</a></th></tr>');
    
    row.addClass(even ? "even" : "odd")    
    row.appendTo('tbody.content');
    request({url: '/'+name}, function ( err, info ) {
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
    , query
    ;
  
  if (window.location.hash.indexOf('?') !== -1) {
    query = window.location.hash.slice(window.location.hash.indexOf('?'))
  }
  
  var init = function () {
    $('h1#topbar').html('<a href="#/">Overview</a><strong>'+db+'</strong>');

    var addquery = function () {
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
        if (startkey[0] !== '"' && startkey.length !== 0) startkey = '"'+startkey+'"'
        if (endkey[0] !== '"' && endkey.length !== 0) endkey = '"'+endkey+'"'
        
         h = '#/'+db+'/_all_docs?';
         if (startkey.length > 0) h += ('startkey='+escape(startkey) + '&');
         if (endkey.length > 0) h += ( 'endkey='+escape(endkey) + '&');
         window.location.hash = h;
      });
    }

    request({url: '/'+db}, function (err, info) {

      for (i in info) {$('div#'+i).text(info[i])}

      var disk_size = info.disk_size;
      $('div#disk_size').text(formatSize(info.disk_size))

      request({url:'/'+db+'/_all_docs?startkey="_design/"&endkey="_design0"'}, function (err, docs) {
        var sizes = [];
        for (var i=0;i<docs.rows.length;i+=1) {
          request({url:'/'+db+'/'+docs.rows[i].id+'/_info?stale=ok'}, function (err, info) {
            if (err) throw err
            sizes.push(info.view_index.disk_size);
            if (sizes.length === docs.rows.length) {
              var s = sum(sizes)
              $('div#views_size').text(formatSize(s));
              $('div#full_size').text(formatSize(s + disk_size));
            }
          })
        }
        if (docs.rows.length === 0) {
          $('div#views_size').text(formatSize(disk_size));
          $('div#full_size').text(formatSize(disk_size));
        }
      })
    })

    $('select.dbquery-select').change(function (e) {
      if (e.target.value === 'all') {
        $('div.alldoc-query').remove();
        if (query) {window.location.hash = '#/'+db}
      } else if (e.target.value === 'ddocs') {
        $('div.alldoc-query').remove();
        addquery();
        $('input#start').val('_design/');
        $('input#end').val('_design0').change();
      } else if (e.target.value === 'query') {
        $('div.alldoc-query').remove();
        addquery();
      }
    })
    if (query) {
      var q = parseQuery(query);
      if (q.startkey || q.endkey) {
        $('option[value=all]').attr('selected', false);
        $('option[value=query]').attr('selected', true).change();
        $('input#start').val(q.startkey ? q.startkey : '');
        $('input#end').val(q.endkey ? q.endkey : '');
      }
    }
  } 
  
  var rowCount = 0;
  var moreRows = function (start, limit) {
    if (query) {
      query += '&limit='+limit+'&skip='+start
    } else {
      query = '?limit='+limit+'&skip='+start
    }
    request({url: '/'+db+'/_all_docs'+query}, function (err, resp) {
      if (err) throw err;
      for (i in resp.rows) {
         var r = resp.rows[i];
         var row = $('<tr><td><a href="#/'+db+'/'+r.key+'">'+r.key+'</a></td><td>' +
                      r.value.rev+'</td></tr>'
                    )
                    .addClass(isEven(rowCount) ? "even" : "odd")
                    .appendTo('tbody.content')
                    ;
         rowCount += 1;
     }
     if (resp.total_rows > (resp.rows.length + start) && !$('span.more').length ) {
       $('td.more').append('<div id="pagination"><span class="more">Load </span><input type="text" id="pages-input" value='+limit+'></input><span class="more"> More Items</span></div>');
     } else if ( resp.total_rows <= (resp.rows.length + start) ) {
       $('div#pagination').remove()
     }
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

app.showDocument = function () {
  var db = this.params['db']
    , docid = this.params['docid']
    ;
  this.render('templates/document.mustache', {db:db,docid:docid}).replace('#content').then(
    $.getScript('script/base64.js', function() {
    $.getScript('script/jquery.resizer.js?0.11.0', function() {
    $.getScript('script/jquery.editinline.js?0.11.0', function() {
    $.getScript('script/jquery.form.js?2.36', function() {
      var page = new $.futon.CouchDocumentPage(db, docid);

      $.futon.navigation.ready(function() {
        this.addDatabase( db );
        // this.updateSelection(
        //   location.pathname.replace(/document\.html/, "database.html"),
        //   "?" + page.db.name
        // );
      });

      $(function() {
        $("h1 a.dbname").text(page.dbName)
          .attr("href", "database.html?" + encodeURIComponent(docid));
        $("h1 strong").text(page.docId);
        $("h1 a.raw").attr("href", "/" + encodeURIComponent(docid) +
          "/" + encodeURIComponent(docid));
        page.updateFieldListing();

        $("#tabs li.tabular a").click(page.activateTabularView);
        $("#tabs li.source a").click(page.activateSourceView);

        $("#toolbar button.save").click(page.saveDocument);
        $("#toolbar button.add").click(page.addField);
        $("#toolbar button.load").click(page.uploadAttachment);
        if (page.isNew) {
          $("#toolbar button.delete").hide();
        } else {
          $("#toolbar button.delete").click(page.deleteDocument);
        }
      });
    });
    });
    });
    })
  )
}

var a = $.sammy(function () {
  
  var indexRoute = function () {
    this.render('templates/index.mustache').replace('#content').then(app.index);
  }
  
  this.get('', indexRoute);
  this.get("#/", indexRoute);
  this.get('#/:db', app.showDatabase);
  this.get('#/:db/_all_docs', app.showDatabase);
  this.get('#/:db/:docid', app.showDocument);
})

$(function () {a.use('Mustache'); a.run(); });