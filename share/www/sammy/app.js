
// -- new style templates --
// this.get('#/', function() {
//   this.render('index.mustache')
//       .replace('#main')
//       .render('items.json')
//       .renderEach('item.mustache')
//       .appendTo('#main ul');
// });

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
app.showDatabase = function (db) {
  $('h1#topbar').html('<a href="#/">Overview</a><strong>'+db+'</strong>');
  request({url: '/'+db}, function (err, info) {
    for (i in info) {$('div#'+i).text(info[i])}
    var disk_size = info.disk_size;
    $('div#disk_size').text(formatSize(info.disk_size))
    
    request({url:'/'+db+'/_all_docs?startkey="_design/"&endkey="design0"'}, function (err, docs) {
      var sizes = [];
      for (var i=0;i<docs.rows.length;i+=1) {
        request({url:'/'+db+'/'+docs.rows[i].id+'/_info'}, function (err, info) {
          sizes.push(info.view_index.disk_size);
          if (sizes.length === docs.rows.length) {
            var s = sum(sizes)
            $('div#views_size').text(formatSize(s));
            $('div#full_size').text(formatSize(s + disk_size));
          }
        })
      }
  })
  
    
  })
  var rowCount = 0;
  var moreRows = function (start, limit) {
    request({url: '/'+db+'/_all_docs?limit='+limit+'&skip='+start}, function (err, resp) {
      if (err) return;
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
       console.log([resp.total_rows, resp.rows.length + start])
       $('div#pagination').remove()
     }
     $('span.more').unbind('click');
     $('span.more').click(function ( ) { moreRows(start + limit, parseInt($('#pages-input').val())) });
   })
  }
  moreRows(0, 20);
}

var a = $.sammy(function () {
  
  var indexRoute = function () {
    this.render('templates/index.mustache').replace('#content').then(app.index);
  }
  var databaseRoute = function () {
    var db = this.params['db'];
    this.render('templates/database.mustache')
        .replace('#content')
        .then( function () {app.showDatabase(db)} )
        ;

  }
  
  this.get('', indexRoute);
  this.get("#/", indexRoute);
  this.get('#/:db', databaseRoute);
})

$(function () {a.run()});