'use strict';

var EE = require('events').EventEmitter;
var util = require('util');
var thrift = require('thrift');
var FramedTransport = require('thrift').TFramedTransport;
var BinaryProtocol = require('thrift').TBinaryProtocol;
var Types = require('./src/bolt_types');
var BoltProxyService = require('./src/BoltProxyService');
var ComputationService = require('./src/ComputationService');

var ComputationMetadata = Types.ComputationMetadata;
var StreamMetadata = Types.StreamMetadata;
//
// Transation returned on each request
//
var Tx = Types.ComputationTx;
var Record = Types.Record;
var Endpoint = Types.Endpoint;

//TODO: Refactor into separate files
exports.Computation = Computation;

//
// Inherit from an event emitter
//
util.inherits(Computation, EE);

function Computation(options) {
  EE.call(this);
  //
  // Remark: Do we want to fail if we dont get a name? For now lets generate
  // a unique identifier
  //
  this.name = options.name || 'node-' + process.hrtime().join('-');

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
      return callback(err);
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
    ? new StreamMetadata({ name: s, grouping: 'SHUFFLE' })
    : new StreamMetadata({ name: s.name, grouping: groupMap(s.grouping) || 'SHUFFLE' });
};

//
// Connect to the client proxy
//
Computation.prototype.connect = function (host, port, callback) {
  var self = this;
  //
  // Set the values and initialize the proxy
  //
  this.host = host;
  this.port = port;
  this.proxy = this.createProxy(host, port);

  this.metadata.proxyEndpoint = new Endpoint({ ip: host, port: port });

  //
  // Alias functions over so they are properly called by the thrift machinery
  //
  this.getState = this.proxy.getState;
  this.setState = this.proxy.setState;

  //
  // Check if there is a result we care about
  //
  this.proxy.registerWithScheduler(this.metadata, function (err) {
    if (err) {
      return callback
        ? callback(err)
        : self.emit('error', err);
    }

    self.emit('register', self.metadata);

    if (callback) callback();
  });

};

//
// Create a bolt service proxy through instantiating a proper thrift connection
//
Computation.prototype.createProxy = function (host, port) {
  this.connection = thrift.createConnection(host, port, {
    transport: FramedTransport(),
    protocol: BinaryProtocol()
  });

  this.connection.on('error', this.emit.bind(this, 'error'));
  this.connection.on('connect', this.emit.bind(this, 'connection'));
  //
  // Return a BoltProxyService
  //
  return thrift.createClient(BoltProxyService, this.connection);
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

//
// Parse environment variables and create a thrift service based on the
// computation
//
exports.run = function run(computation) {
  var server = thift.createServer(ComputationService, computation);

  //
  // Send a start event when we are fully connected to everything
  //
  var finish = after(2, function () {
    server.emit('start');
  });

  //
  // Port to listen on
  //
  var port = process.env[Types.kConcordEnvKeyClientListenAddr].split(':')[1];
  //
  // Grab address to start proxy connection
  //
  var proxyParams = process.env[Types.kConcordEnvKeyClientProxyAddr].split(':');

  //
  // Error if we do not have environment variables set
  //
  if (!port || !proxyParams.length) {
    setImmediate(server.emit.bind(emitter), 'error', new Error('Environment variables not found'));
    return server;
  }
  //
  // So we can apply an array as the arguments, we use .apply
  //
  computation.connect.apply(computation, proxyParams);
  computation.once('connect', finish);
  server.once('connect', finish);

  server.listen(port);

  return server;
};
