Welcome to the world of GeoCouch
================================

Prerequisites
-------------

Clone the GeoCouch branch:

    # default branch is already geocouch
    git clone http://github.com/vmx/couchdb.git
    cd couchdb

Compile it:

    ./bootstrap
    ./configure
    make dev

Run it:

    ./utils/run


Using GeoCouch
--------------

Create a database:

    curl -X PUT http://127.0.0.1:5984/places

Add a Design Document with a spatial function:

    curl -X PUT -d '{"spatial":{"points":"function(doc) {\n    if (doc.loc) {\n        emit(doc._id, {\n            type: \"Point\",\n            coordinates: [doc.loc[0], doc.loc[1]]\n        });\n    }};"}}' http://127.0.0.1:5984/places/_design/main

Put some data into it:

    curl -X PUT -d '{"loc": [-122.270833, 37.804444]}' http://127.0.0.1:5984/places/oakland
    curl -X PUT -d '{"loc": [10.898333, 48.371667]}' http://127.0.0.1:5984/places/augsburg

Make a bounding box request:

    curl -X GET 'http://localhost:5984/places/_design/main/_spatial/points/%5B0,0,180,90%5D'
    
It should return:

    {"query1":[{"id":"augsburg","loc":[10.898333,48.371667]}]}


The Design Document Function
----------------------------

function(doc) {
    if (doc.loc) {
        emit(doc._id, {
            type: "Point",
            coordinates: [doc.loc[0], doc.loc[1]]
        });
    }};"

It uses the emit() from normal views. The key isn't taken into account, it
could be `null`. The value needs to be [GeoJSON](http://geojson.org). At
the moment only points are supported.
