'use strict';

var thrift = require('thrift');
var through = require('through2');
var Types = require('./src/bolt_types');
var BoltManagerService = require('./src/BoltManagerService');
var BoltProxyService = require('./src/BoltProxyService');
var BoltSchedulerService = require('./src/BoltSchedulerService');

var ComputationMetadata = Types.ComputationMetadata;
var StreamMetadata = Types.StreamMetadata;
//
// Transation returned on each request
//
var Tx = Types.ComputationTx;
var Record = Types.Record;
var Endpoint = Types.Endpoint;


function Computation(options) {
  //
  // Remark: Do we want to fail if we dont get a name? For now lets generate
  // a unique identifier
  //
  this.name = options.name || 'node-' + process.hrtime().join('-');
  //
  // Set the grouping to the correct INT code based on what we have in the
  // thrift types. We default to SHUFFLE
  //
  this.grouping = groupMap(options.grouping || 'shuffle');

  if (!this.grouping) throw new Error('Invalid grouping ' + options.grouping);

  //
  // Computation init function and process functions which are required
  //
  this._init = typeof options.init === 'function' ? options.init : undefined;
  this._processRecord = typeof options.processRecord === 'function'
    ? options.processRecord
    : undefined;

  this.processTimer = typeof options.processTimer === 'function'
    ? options.processTimer
    : undefined;

  if (!this._init || !this._processRecord) {
    throw new Error('You must implement the init and processRecord functions and pass them in');
  }

  //
  // TODO: Validate these arrays for the case that they are objects as they
  // require both a name and grouping property
  //
  this.publish = options.publish || [];
  this.subscribe = options.subscribe || [];

  if (!this.publish.length && !this.subscribe.length) {
    throw new Error('You must subscribe and/or publish to certain named messages');
  }

  //
  // Derive the metadata object that we use to communicate with concord from
  // the subscribe and publish options
  //
  this.metadata = this.deriveMeta();
}

//
// THESE FUNCTIONS CANNOT CHANGE! They are called by the thrift craziness.
// These functions are used to proxy the calls to the user defined functions
// when they define their computation
//
// BEGIN BLOCK!
//

//
// Call the init function. This allows the user to set any timers
// or produce any records even before they receive anything. More useful for
// setting timers
//
Computation.prototype.init = function (callback) {
  var tx = new Transaction();
  var ctx = new Context(tx);

  this._init.call(ctx, function (err) {
    return err
      ? callback(err)
      : callback(null, tx);
  });
};

Computation.prototype.boltProcessRecord = function (record, callback) {
  var self = this;
  var tx = new Transaction();
  var ctx = new Context(tx);

  //
  // For the functions that
  //
  this._processRecord.call(ctx, record, function (err) {
    if (err) {
      retur callback(err);
    }

    //
    // We ALWAYS return the transaction here to be sent over the thrift
    // protocol
    //
    callback(null , tx);
  });
};

Computation.prototype.boltProcessTimer = function (key, time, callback) {
  var tx = new Transaction();
  var ctx = new Context(tx);

  this._processTimer.call(ctx, key, time, function (err) {
    if (err) { return callback(err); }
    callback(null, tx);
  });
};

//
// This just always returns the derived metadata
//
Computation.prototype.boltMetadata = function (callback) {
  return void callback(null, this.metadata);
};

//
// END BLOCK!
//

//
// Default the user defined versions of these functions
//
['_init', '_processRecord', '_processTimer'].forEach(function (action) {
  Computation.prototype[action] = function () {
    throw new Error(action.slice(1) + ' must be implemented by passing it into the constructor');
  };
});

//
// This derives the metadata object that we communicate to concord
//
Computation.prototype.deriveMeta = function () {
  return new ComputationMetadata({
    name: this.name,
    istreams: this.subscribe.map(this._mapMetadata, this),
    ostreams: this.publish.map(this._mapMetadata, this),
  });
};

//
// Return proper stream meta data object given strings or objects
//
Computation.prototype._mapMetadata = function (s) {
  return typeof s === 'string'
    ? new StreamMetadata({ name: s, grouping: this.grouping })
    : new StreamMetadata({ name: s.name, grouping: groupMap(s.grouping) || this.grouping });
};

Computation.prototype.createProxy = function () {

};

//
// A wrapper around the thrift ComputationTx to initialize values
//
function Transaction() {
  return new Tx({
    records: [],
    timers: {}
  });
}
//
// A context that wraps the transaction and provides methods to modify each
// transaction sent to concord
//
function Context(tx) {
  this.__tx = tx;
}

//
// Accepts the stream name, the key and the value
//
Context.prototype.produceRecord = function (stream, key, value) {
  this.__tx.records.push(new Record({
    key: key,
    value: value,
    userStream: stream
  }));
};

Context.prototype.setTimer = function (key, time) {
  this.__tx.timers[key] = time;
};

//
// Handle mapping a grouping name to the proper TYPE, otherwise return an error
//
function groupMap(grouping) {
  return Types.streamGrouping[snakeCase(grouping).toUpperCase()]
}

//
// Naive snakeCase implementation
//
function snakeCase(text) {
  return text.replace(/[a-z][A-Z]/g, function (ch) {
    return ch[0] + '_' + ch[1].toLowerCase();
  });
};
