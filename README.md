# concord-node
nodejs driver for concord.io

## Install

```sh
# When it exists
npm install concord --save
```

## Usage
Implement a computation

```js
var concord = require('concord');

//
// Define a computation with the given functions needed. You can pass in the
// parameters or do standard inheritance on the computation and implement the
// `init` and `processRecords` function. A client connection exists on the
// computation currently which talks to the local proxy this computation does work
// with. In the future this should be abstracted out if possible
//
var computation = new concord.Computation({
  name: 'processor',
  //
  // Set the grouping for this computation
  //
  grouping: 'shuffle',
  //
  // This can either be an array of objects or an array of string literals
  // Depending if you want to specify a specific grouping per what you are
  // subscribing to
  //
  subscribe: ['words'],
  publish: ['inserter'],
  //
  // Define these functions. Use the `this` context to call the available
  // functions within the implemented functions.
  //
  init: function (callback) {
    //
    // Do some asynchronous actions using the `this` context. We define
    // 2 methods, `setTimer` and `produceRecord`
    //
    this.setTimer(name, key, 1000);
  },
  processRecord: function (record, callback) {
    this.produceRecord('words', key, value);

    callback();
  },
  processTimer: function (key, time, callback) {
    //
    // Process an aggregate after X time
    //
    this.produceRecord('words', key, computedValue);
    callback();
  }
});

//
// Network errors for the thrift connection are handled here
//
computation.on('error', function (err) {
  console.error(err);
});

computation.on('register', function (meta) {
  console.log('We have registered with proxy with %j', meta);
});

//
// This is how we know we are connected to the proxy
//
computation.on('connect', function () {
  console.log('connected to proxy!');
});

//
// Run the computation! Returns the computation server
//
var server = concord.run(computation);

server.on('error', function (err) {
  console.error('thrift processing server errored');
});

//
// This means we have connected to both proxy and are listening on a port to
// receive computations
//
server.on('start', function () {
  console.log('We have begun!')
});


```
