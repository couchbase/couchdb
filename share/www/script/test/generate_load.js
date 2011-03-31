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

couchTests.generate_load = function(debug) {
    xhr = CouchDB.newXhr();

    //verify the hearbeat newlines are sent
    xhr.open("POST", "/_generate_load?total=100000&db=test&concurrency=5&batch=100", false);
    var base64bin = "";
    for (var i=0; i<34; i++) {
        base64bin += "MTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTEx\r\n"
    }
    base64bin += "MTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTEx";
    
    xhr.send(JSON.stringify({damien:"rules",
        _attachments:{"foo.txt": {content_type:"application/binary",data:base64bin}}
            }));
};
