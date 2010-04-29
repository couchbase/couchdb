Welcome to the World of GeoCouch
================================

Create a database:

    curl -X PUT http://127.0.0.1:5984/places

Add a Design Document with a spatial function:

    curl -X PUT -d '{"spatial":{"points1":"function(doc) {\n    if (doc.loc) {\n        emit(doc._id, {\n            type: \"Point
\",\n            coordinates: [doc.loc[0], doc.loc[1]]\n        });\n    }};"}}' http://127.0.0.1:5984/places/_design/main

Put some data into it:

    curl -X PUT -d '{"loc": [-122.270833, 37.804444]}' http://127.0.0.1:5984/places/oakland
    curl -X PUT -d '{"loc": [10.898333, 48.371667]}' http://127.0.0.1:5984/places/augsburg

Make a bounding box request:

    curl -X GET 'http://localhost:5984/places2/_design/default/_spatial/hello/%5B0,0,180,90%5D'
