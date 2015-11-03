var concord = require('concord');
var Computation = concord.Computation;
var util = require('util');

var comp = new Computation({
  name: 'word-counter',
  init: function (ctx, callback) {
    console.log("Initializing");
    this.words = {};
    this.idx = 0;
    this.isSet = false;
  },
  processRecord: function (ctx, record, callback) {
    var count = this.words[record.key] = (this.words[record.key] || 0) + 1;
    this.idx++;

    if (this.idx % 10000 == 0) {
      console.log(this.idx);
    }

    if (!this.isSet) {
      var time = (new Date()).getTime() + 1000;
      ctx.setTimer("test", time);
    }

    callback();
  },
  processTimer: function (ctx, key, time, callback) {
    console.log("Timer triggered");
    callback();
  },
  subscribe: ['words']
});

concord.run(comp);

