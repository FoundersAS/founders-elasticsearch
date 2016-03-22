'use strict';

const elasticsearch = require('elasticsearch');
const through = require('through2');
const highland = require('highland');

module.exports = function (options) {
  const _INDEX = options.index;
  const _TYPE = options.type;

  const that = {};
  const client = new elasticsearch.Client(options);

  that.get = function (id, cb) {
    client.get({
      index: _INDEX,
      type: _TYPE,
      id: id
    }, cb);
  };

  that.create = function (id, body, cb) {
    client.create({
      index: _INDEX,
      type: _TYPE,
      id,
      body
    }, cb);
  };

  that.update = function (id, doc, cb) {
    client.update({
      index: _INDEX,
      type: _TYPE,
      doc_as_upsert: true,
      id,
      body: {
        doc
      }
    }, cb);
  };

  that.search = function (body, cb) {
    client.search({
      index: _INDEX,
      type: _TYPE,
      body
    }, cb);
  };

  that.searchByMessageId = function (messageId, cb) {
    client.search({
      index: _INDEX,
      type: _TYPE,
      body: {
        query: {
          match: {
            messageIds: messageId
          }
        }
      }
    }, cb);
  };

  that.getStream = function (id) {
    return highland(function (push, next) {
      client.get({
        index: _INDEX,
        type: _TYPE,
        id: id
      }, function (err, results) {
        if (err && err.status !== 404) return push(err);

        push(null, results);
        push(null, highland.nil);
      });
    });
  };

  that.mGetStream = function (ids) {
    return highland(function (push, next) {
      client.mget({
        index: _INDEX,
        type: _TYPE,
        body: {
          ids
        }
      }, function (err, results) {
        if (err && err.status !== 404) return push(err);

        push(null, results);
        push(null, highland.nil);
      });
    });
  };

  that.queryStream = function (data) {
    let buff;

    return highland(function (push, next) {
      if (buff && !buff.length) return push(null, highland.nil);
      if (buff && buff.length) {
        push(null, buff.shift(1));
        return next();
      }

      client.search({
        index: _INDEX,
        type: _TYPE,
        body: data.body
      }, function (err, results) {
        if (err) return push(err);
        buff = results.hits.hits;
        push(null, buff.shift(1));
        next();
      });
    });
  };

  that.createStream = function (passThrough) {
    if (passThrough === undefined) passThrough = false;

    return through.obj({highWaterMark: 0}, function (data, enc, cb) {
      client.create({
        index: _INDEX,
        type: _TYPE,
        id: data.id,
        body: data.body
      }, function (err, results) {
        if (err) return cb(err);
        if (passThrough) return cb(null, results);
        cb();
      });
    });
  };

  that.updateStream = function (doc_as_upsert, passThrough) {
    if (doc_as_upsert === undefined) doc_as_upsert = true;
    if (passThrough === undefined) passThrough = false;

    return through.obj({highWaterMark: 0}, function (data, enc, cb) {
      client.update({
        index: _INDEX,
        type: _TYPE,
        doc_as_upsert: doc_as_upsert,
        id: data.id,
        body: data.body
      }, function (err, results) {
        if (err) return cb(err);
        if (passThrough) return cb(null, data, results);
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
