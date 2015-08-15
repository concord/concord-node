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
parameters or do standard inheritance on the computation and implement the
`init` and `processRecords` function.
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
  // Define these functions
  //
  init: function () {},
  processRecord: function () {}
});

//
// Abstract the server so we keep the connection handling in one spot
//
var server = new concord.Server();
server.run(computation);



```
