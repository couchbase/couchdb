/**
 * @copyright 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/

function sum(values) {
    var sum = 0;
    for (var i = 0; i < values.length; ++i) {
        sum += values[i];
    }
    return sum;
};

// I wish it was on the prototype, but that will require bigger
// C changes as adding to the date prototype should be done on
// process launch. The code you see here may be faster, but it
// is less JavaScripty.
// "Date.prototype.toArray = (function() {"
function dateToArray(date) {
    date = date.getUTCDate ? date : new Date(date);
    return isFinite(date.valueOf()) ?
      [date.getUTCFullYear(),
      (date.getUTCMonth() + 1),
       date.getUTCDate(),
       date.getUTCHours(),
       date.getUTCMinutes(),
       date.getUTCSeconds()] : null;
};

function decodeBase64(b64) {
    var i, j, l, tmp, scratch, arr = [];
    var lookup = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
    if (typeof b64 !== 'string') {
        throw 'Input is not a string';
    }
    if (b64.length % 4 > 0) {
        throw 'Invalid base64 source.';
    }
    scratch = b64.indexOf('=');
    scratch = scratch > 0 ? b64.length - scratch : 0;
    l = scratch > 0 ? b64.length - 4 : b64.length;
    for (i = 0, j = 0; i < l; i += 4, j += 3) {
        tmp = (lookup.indexOf(b64[i]) << 18) | (lookup.indexOf(b64[i + 1]) << 12);
        tmp |= (lookup.indexOf(b64[i + 2]) << 6) | lookup.indexOf(b64[i + 3]);
        arr.push((tmp & 0xFF0000) >> 16);
        arr.push((tmp & 0xFF00) >> 8);
        arr.push(tmp & 0xFF);
    }
    if (scratch === 2) {
        tmp = (lookup.indexOf(b64[i]) << 2) | (lookup.indexOf(b64[i + 1]) >> 4);
        arr.push(tmp & 0xFF);
    } else if (scratch === 1) {
        tmp = (lookup.indexOf(b64[i]) << 10) | (lookup.indexOf(b64[i + 1]) << 4);
        tmp |= (lookup.indexOf(b64[i + 2]) >> 2);
        arr.push((tmp >> 8) & 0xFF);
        arr.push(tmp & 0xFF);
    }
    return arr;
};
