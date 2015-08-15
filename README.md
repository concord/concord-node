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
// Abstract the server so we keep the connection handling in one spot
// Optionally pass in parameters, it will automatically detect them from the
// environment
//
var server = new concord.Server();
server.run(computation);



```
