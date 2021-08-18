'use strict';

var inherits = require('util').inherits;
var StorageAdapter = require('../adapter');
var StorageItem = require('../item');
var path = require('path');
var assert = require('assert');
var fs = require('fs');
var utils = require('../../utils');
var mkdirp = require('mkdirp');
var Stream = require('stream');

/**
 * Implements a File storage adapter interface
 * @extends {StorageAdapter}
 * @param {String} storageDirPath - Absolute path to store the files
 * @constructor
 */
function FileStorageAdapter(storageDirPath) {
  if (!(this instanceof FileStorageAdapter)) {
    return new FileStorageAdapter(storageDirPath);
  }

  this._validatePath(storageDirPath);

  this._path = storageDirPath;

  this._isOpen = true;

  this._lastTimeSize = null;
  this._storageSize = 0;
}

inherits(FileStorageAdapter, StorageAdapter);

/**
 * Validates the storage path supplied
 * @private
 */
FileStorageAdapter.prototype._validatePath = function (storageDirPath) {
  if (!utils.existsSync(storageDirPath)) {
    mkdirp.sync(storageDirPath);
  }

  assert(utils.isDirectory(storageDirPath), 'Invalid directory path supplied');
};

/**
 * Implements the abstract {@link StorageAdapter#_get}
 * @private
 * @param {String} key
 * @param {Function} callback
 */
FileStorageAdapter.prototype._get = function (key, callback) {
  var self = this;
  const exist = utils.existsSync(path.join(self._path, key))
  if(exist){ 
    self._peek(key, (err, value) => {
      if (err) {
        return callback(err)
      }
      callback(null, value)
    })
  }else {
    var newItem = new StorageItem({});
    self._put(key, newItem , (err) => {
      if(err) return callback(err)
      callback(null, newItem);
    })
  }
};

/**
 * Implements the abstract {@link StorageAdapter#_peek}
 * @private
 * @param {String} key
 * @param {Function} callback
 */
FileStorageAdapter.prototype._peek = function (key, callback) {
  fs.readFile(path.join(self._path, key), 'utf8' , (err, value) => {
    if (err) {
        return callback(err)
    }
    callback(null, JSON.parse(value));
  })
};

/**
 * Implements the abstract {@link StorageAdapter#_put}
 * @private
 * @param {String} key
 * @param {Object} item
 * @param {Function} callback
 */
FileStorageAdapter.prototype._put = function (key, item, callback) {
  var self = this;

  item.shard = null; // NB: Don't store any shard data here

  item.fskey = utils.ripemd160(key, 'hex');

  fs.writeFile(path.join(self._path, key), JSON.stringify(item), function(err) {
    if(err) {
        return callback(err);
    }
    callback(null);
  }); 
};

/**
 * Implements the abstract {@link StorageAdapter#_del}
 * @private
 * @param {String} key
 * @param {Function} callback
 */
FileStorageAdapter.prototype._del = function (key, callback) {
  var self = this;
  if (utils.existsSync(path.join(self._path, key))) {
    fs.unlink(path.join(self._path, key), (err) => {
        if (err) {
            return callback(err)
        }
        callback(null)
    })
  }else callback(null)
};

/**
 * Implements the abstract {@link StorageAdapter#_flush}
 * @private
 * @param {Function} callback
 */
FileStorageAdapter.prototype._flush = function (callback) {
  // No flush implementation for file storage
  callback(null);
};

/**
 * Implements the abstract {@link StorageAdapter#_size}
 * @private
 * @param {String} [key]
 * @param {Function} callback
 */
FileStorageAdapter.prototype._size = function (key, callback) {
  var self = this;

  const getAllFiles = function(dirPath) {
    const files = fs.readdirSync(dirPath)
  
    let arrayOfFiles = arrayOfFiles || []
  
    files.forEach(function(file) {
      if (!fs.statSync(dirPath + "/" + file).isDirectory()) arrayOfFiles.push(path.join(dirPath, file))
    })
  
    return arrayOfFiles
  }

  if (typeof key === 'function') {
    callback = key;
    key = null;
  }

  if (key) callback(null, fs.statSync(path.join(self._path, key)).size)
  else {
    const arrayOfFiles = getAllFiles(self._path)
    let totalSize = 0

    arrayOfFiles.forEach(function(filePath) {
      totalSize += fs.statSync(filePath).size
    })

    callback(null, totalSize) 
  }
};

/**
 * Implements the abstract {@link StorageAdapter#_keys}
 * @private
 * @returns {ReadableStream}
 */
FileStorageAdapter.prototype._keys = function () {
  var self = this;
  const readable = new Stream.Readable()

  readable.pipe(process.stdout)

  const files = fs.readdirSync(self._path)
  const items = [];

  files.forEach(function(file) {
    if (!fs.statSync(dirPath + "/" + file).isDirectory()) items.push(path.join(dirPath, file))
  })

  items.forEach(item => readable.push(item))

  // no more data
  readable.push(null)
  return readable
};

/**
 * Implements the abstract {@link StorageAdapter#_open}
 * @private
 * @param {Function} callback
 */
FileStorageAdapter.prototype._open = function (callback) {
  callback(null);
};

/**
 * Implements the abstract {@link StorageAdapter#_close}
 * @private
 * @param {Function} callback
 */
FileStorageAdapter.prototype._close = function (callback) {
  callback(null);
};

module.exports = FileStorageAdapter;
