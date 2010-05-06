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

    curl -X PUT -d '{"spatial":{"points":"function(doc) {\n    if (doc.loc) {\n        emit({\n            type: \"Point\",\n            coordinates: [doc.loc[0], doc.loc[1]]\n        }, doc._id);\n    }};"}}' http://127.0.0.1:5984/places/_design/main

Put some data into it:

    curl -X PUT -d '{"loc": [-122.270833, 37.804444]}' http://127.0.0.1:5984/places/oakland
    curl -X PUT -d '{"loc": [10.898333, 48.371667]}' http://127.0.0.1:5984/places/augsburg

Make a bounding box request:

    curl -X GET 'http://localhost:5984/places/_design/main/_spatial/points/%5B0,0,180,90%5D'
    
It should return:

    {"spatial":[{"id":"augsburg","bbox":[10.898333,48.371667,10.898333,48.371667],"value":"augsburg"}]}


The Design Document Function
----------------------------

function(doc) {
    if (doc.loc) {
        emit({
            type: "Point",
            coordinates: [doc.loc[0], doc.loc[1]]
        }, doc._id);
    }};"

It uses the emit() from normal views. The key is a
[GeoJSON](http://geojson.org) geometry, the value is any arbitrary JSON. All
geometry types (even GemetryCollections) are supported.

If the GeoJSON geometry contains a `bbox` property it will be used instead
of calculating it from the geometry (even if it's wrong, i.e. is not
the actual bounding box).
