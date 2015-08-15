'use strict';

var thrift = require('thrift');
var Types = require('./src/bolt_types');
var BoltManagerService = require('./src/BoltManagerService');
var BoltProxyService = require('./src/BoltProxyService');
var BoltSchedulerService = require('./src/BoltSchedulerService');

var ComputationMetadata = Types.ComputationMetadata;
var StreamMetadata = Types.StreamMetadata;

function Computation (options) {
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
  this.init = typeof options.init === 'function' ? options.init : undefined;
  this.processRecord = typeof options.processRecord === 'function'
    ? options.processRecord
    : undefined;

  if (!this.init || !this.processRecord) {
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
}

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
