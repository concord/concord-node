'use strict';

var thrift = require('thrift');

var BoltProxyService = require('./src/BoltProxyService');
var ComputationService = require('./src/ComputationService');
var Types = require('./src/bolt_types');
var ComputationMetadata = Types.ComputationMetadata;
var Promise = require('promise');
var after = require('after');

function Computation(options) {
  var self = this;
  self.handler = {};

  var proxyParams = process.env[Types.kConcordEnvKeyClientProxyAddr].split(':');
  proxyParams[1] = parseInt(proxyParams[1]);

  options.proxyHost = proxyParams[0];
  options.proxyPort = proxyParams[1];
 
  var fields = [
    ['name', 'string', true],
    ['init', 'function'],
    ['processRecord', 'function'],
    ['processTimer', 'function'],
    ['publish', 'object'],
    ['subscribe', 'object'],
    ['proxyHost', 'string', true],
    ['proxyPort', 'number', true]
  ];

  var processField = function (name, type, required) {
    if (options[name] !== undefined) {
      if (typeof options[name] === type) {
        self.handler[name] = options[name];
      } else {
        throw new Error('Field ' + name + ' must be of type ' + type);
      }
    } else if (required) {
      throw new Error('Field ' + name + ' is required');
    }
  };

  fields.forEach(function (args) { processField.apply(self, args); });

  self.metadata = this.deriveMetadata();
  self.proxy = {connected: false};
}

Computation.prototype.getProxy = function () {
  if (!this.proxy.connected) {
    var self = this;
    this.proxy.client = new Promise(function (resolve, reject) {
      var transport = new thrift.TFramedTransport();
      var protocol = new thrift.TBinaryProtocol(transport);

      var conn = thrift.createConnection(
        this.handler.proxyHost,
        this.handler.proxyPort,
        {transport: transport, protocol: protocol});

      console.error('lol');
      
      var client = thrift.createClient(BoltProxyService, conn);

      console.error('made conn', conn);

      conn.on('connect', function () {
        console.error('Creating client...');
        self.proxy.connected = true;
        resolve(client);
      });
      //conn.on('error', reject.bind(self));
      conn.on('error', function () {
        console.error('Error creating client...');
      });
    });
  }

  console.error('returning get me outta here');
  return this.proxy.client;
};

Computation.prototype.createComputationContext = function () {
  var self = this;
  var tx = new Types.ComputationTx();

  var produceRecord = function (stream, key, value) {
    tx.records.push(new Types.Record({
      key: key,
      data: data,
      userStream: stream
    }));
  };

  var setTimer = function (name, time) {
    tx.timers[name] = time;
  };

  var getState = function (key) {
    return self.getProxy().then(function (proxy) {
      return new Promise(function (resolve, reject) {
        proxy.getState(key, function (err, value) {
          if (err) {
            reject(err);
          } else {
            resolve(value);
          }
        });
      });
    });
  };

  var setState = function (key, value) {
    return self.getProxy().then(function (proxy) {
      return new Promise(function (resolve, reject) {
        proxy.setState(key, value, function (err) {
          if (err) {
            reject(err);
          } else {
            resolve();
          }
        });
      });
    });
  };

  var ctx = {
    produceRecord: produceRecord,
    setTimer: setTimer,
    getState: getState,
    setState: setState
  };

  return [tx, ctx];
};

Computation.prototype.deriveMetadata = function() {
  var metadata = new ComputationMetadata();

  metadata.name     = this.handler.name;
  metadata.ostreams = this.handler.publish || [];
  this.handler.subscribe = this.handler.subscribe || [];
  metadata.istreams = this.handler.subscribe.map(function (stream) {
    return new Types.StreamMetadata(stream);
  });

  if (!metadata.ostreams.length && !metadata.istreams.length) {
    throw new Error("Must list input or output streams");
  }

  metadata.proxyEndpoint = new Types.Endpoint({
    host: this.handler.proxyHost,
    port: this.handler.proxyPort
  });

  return metadata;
};

Computation.prototype.init = function (result) {
  var txInfo = this.createComputationContext();
  var tx     = txInfo[0];
  var ctx    = txInfo[1];
  try {
    this.handler.init(ctx);
    result(null, tx);
  } catch (e) {
    result(e.toString());
  }
};

Computation.prototype.metadata = function (result) {
  result(null, this.metadata);
};

Computation.prototype.boltProcessRecords = function (result, records) {
  var txs = [];
  records.map(function (record) {
    var txInfo = this.createComputationContext();
    var tx     = txInfo[0];
    var ctx    = txInfo[1];

    this.handler.produceRecord(ctx, record);

    txs.push(tx);
  });

  result(null, txs);
};

Computation.prototype.boltProcessTimer = function (result, key, time) {
  var txInfo = this.createComputationContext();
  var tx     = txInfo[0];
  var ctx    = txInfo[1];

  this.handler.processTimer(ctx, key, time);

  result(null, tx);
};

exports.Computation = Computation;
exports.run = function (computation) {
  var listenPort = process.env[Types.kConcordEnvKeyClientListenAddr].split(':')[1];

  var server = thrift.createServer(ComputationService, computation, {
    transport: thrift.TFramedTransport,
    protocol: thrift.TBinaryProtocol
  });

  server.once('listening', function () {
    console.error('Server listening, registering with scheduler');
    computation.getProxy().then(function (proxy) {
      proxy.registerWithScheduler(computation.metadata, function (err) {
        if (err) {
          console.error('ERROR: Failed to register:', err);
        }
      });
    });
  });

  server.listen(listenPort);

  return server;
};

