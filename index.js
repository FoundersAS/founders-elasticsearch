'use strict';

const elasticsearch = require('elasticsearch');
const through = require('through2');
const highland = require('highland');

module.exports = function (opts) {
  const that = {};
  that.client = new elasticsearch.Client(opts);

  that.getStream = function (opts) {
    return highland(function (push, next) {
      that.client.get(opts, function (err, results) {
        if (err && err.status !== 404) return push(err);

        push(null, results);
        push(null, highland.nil);
      });
    });
  };

  that.mGetStream = function (opts) {
    return highland(function (push, next) {
      that.client.mget(opts, function (err, results) {
        if (err && err.status !== 404) return push(err);

        push(null, results);
        push(null, highland.nil);
      });
    });
  };

  that.queryStream = function (opts) {
    let buff;

    return highland(function (push, next) {
      if (buff && !buff.length) return push(null, highland.nil);
      if (buff && buff.length) {
        push(null, buff.shift(1));
        return next();
      }

      that.client.search(opts, function (err, results) {
        if (err) return push(err);
        buff = results.hits.hits;
        push(null, buff.shift(1));
        next();
      });
    });
  };

  that.createStream = function (opts) {
    return through.obj({highWaterMark: 0}, function (data, enc, cb) {
      that.client.create(Object.assign(opts, data), function (err, results) {
        if (err) return cb(err);
        if (opts.passThrough) return cb(null, { data, results });
        cb();
      });
    });
  };

  that.updateStream = function (opts) {
    return through.obj({highWaterMark: 0}, function (data, enc, cb) {
      that.client.update(Object.assign(opts, data), function (err, results) {
        if (err) return cb(err);
        if (opts.passThrough) return cb(null, { data, results });
        cb();
      });
    });
  };

  that.logResult = function (cb) {
    return through.obj({highWaterMark: 0}, function (data, enc, cb) {
      console.log(data);
      cb();
    });
  };

  return that;
};
