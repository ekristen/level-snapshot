var fs = require('fs')
var net = require('net')
var path = require('path')
var util = require('util')
var events = require('events')

var async = require('async')
var after = require('after')
var xtend = require('xtend')
var mkdirp = require('mkdirp')
var rimraf = require('rimraf')
var through2 = require('through2')
var timestamp = require('monotonic-timestamp')
var protobufs = require('protocol-buffers-stream')

var debug = require('debug')('level-snapshot')
var debugc = require('debug')('level-snapshot:client')
var debugs = require('debug')('level-snapshot:server')

var LevelSnapshot = module.exports = function (db, opts) {
  if (db._snapshot) {
    return
  }

  if (!(this instanceof LevelSnapshot)) {
    return new LevelSnapshot(db, opts)
  }

  if (typeof db.db.liveBackup !== 'function') {
    throw new Error('level-snapshot requires level-hyper')
  }

  this.opts = xtend({
    path: './snapshots',
    logPath: './logs',
    interval: 3600,
    lastSyncPath: './lastsync',
    noSnapshot: false
  }, opts)

  this.db = db
  this._snapshotInterval = null

  this._logStream = through2()
  this._logStreamNull = through2.obj(function (chunk, enc, callback) {
    // Dump any logStream stuff until we roll for the first time.
    callback()
  })
  this._logStream.pipe(this._logStreamNull)

  this._logStreamCurrent = null

  var schema = fs.readFileSync(path.join(__dirname, 'schema.proto'))
  this.protocols = protobufs(schema)

  mkdirp.sync(this.opts.path)
  mkdirp.sync(this.opts.logPath)

  this.setupMethods()
  this.setupEvents()

  this.attach()

  return this
}
util.inherits(LevelSnapshot, events.EventEmitter)

LevelSnapshot.prototype.setupEvents = function () {
  var self = this

  this.on('snapshot:start', function (snapshotName) {
    self.roll(snapshotName)
  })

  this.on('snapshot:error', function (err) {
    console.log('snapshot:error', err)
  })

  this.on('snapshot:cleanup', function (snapshotName) {
    var fileName = path.join(self.opts.logPath, snapshotName)
    fs.unlink(fileName, function (err) {
      if (err) console.log('failed to remove', fileName)
    })
  })
}

LevelSnapshot.prototype.setupMethods = function () {
  var self = this

  this.db.methods = this.db.methods || {}

  if (typeof this.db.methods.liveBackup !== 'undefined') {
    return
  }

  this.db.methods.liveBackup = { type: 'async' }
  this.db.liveBackup = function (dir, cb) {
    if (typeof dir === 'number') {
      dir = String(dir)
    }

    self.db.db.liveBackup(dir, cb)
  }
}

LevelSnapshot.prototype.roll = function (snapshotName) {
  var filePath = path.join(this.opts.logPath, snapshotName)

  this._logStream.unpipe(this._logStreamNull)

  var previous = this._logStreamCurrent
  this._logStreamCurrentFile = filePath
  this._logStreamCurrent = fs.createWriteStream(filePath)
  this._logStream.pipe(this._logStreamCurrent)

  if (previous !== null) {
    this._logStream.unpipe(previous)
    previous.close()
  }
}

LevelSnapshot.prototype.attach = function () {
  var self = this

  function createLogStream () {
    var readStream = through2()
    fs.createReadStream(this._logStreamCurrentFile).pipe(readStream, {end: false})
    self._logStream.pipe(readStream, {end: false})
    return readStream
  }

  function put (db, key, value, options, callback) {
    if (typeof options === 'function') {
      callback = options
      options = {}
    }
    write({ type: 'put', key: key, value: value, options: options })
    self.db._snapshot.put.call(db, key, value, options, callback)
  }

  function del (db, key, options, callback) {
    if (typeof options === 'function') {
      callback = options
      options = {}
    }
    write({ type: 'del', key: key, options: options })
    self.db._snapshot.del.call(db, key, options, callback)
  }

  function write (data) {
    self._logStream.write(JSON.stringify(data) + '\n')
  }

  // TODO batch!
  this.db._snapshot = {
    put: this.db.put.bind(this.db),
    del: this.db.del.bind(this.db),
    createLogStream: createLogStream.bind(this)
  }

  // TODO batch!
  this.db.put = put.bind(null, this.db)
  this.db.del = del.bind(null, this.db)
}

LevelSnapshot.prototype.start = function () {
  if (this._snapshotInterval !== null) {
    return debug('snapshots already started')
  }

  var self = this

  function expireSnapshots (callback) {
    fs.readdir(self.opts.path, function (err, files) {
      if (err) return callback(err)
      var len = files.length - 4
      if (len <= 0) {
        return callback()
      }
      var done = after(len, callback)
      for (var i = 0; i < len; i++) {
        (function (index) {
          var file = files[index]
          var filePath = path.join(self.opts.path, file)
          rimraf(filePath, function (err) {
            if (err) {
              return done(err)
            }
            self.emit('snapshot:cleanup', file)
            done()
          })
        })(i)
      }
    })
  }

  function doSnapshot () {
    var backupName = ['snapshot', timestamp()].join('-')
    var backupPath = path.join(self.db.location, ['backup', backupName].join('-'))

    self.emit('snapshot:start', backupName)

    self.db.db.liveBackup(String(backupName), function () {
      var snapshotPath = path.join(self.opts.path, backupName)
      var currentPath = path.join(self.opts.path, 'snapshot-current')

      // Move Backup to Snapshot Directory
      fs.rename(backupPath, snapshotPath, function (err) {
        if (err) {
          return self.emit('snapshot:error', err)
        }

        // Remove Link to Old Snapshot
        fs.unlink(currentPath, function (err) {
          if (err && err.code !== 'ENOENT') {
            return self.emit('snapshot:error', err)
          }

          // Link
          fs.symlink(path.basename(snapshotPath), currentPath, function (err) {
            if (err) {
              return self.emit('snapshot:error', err)
            }

            // Expire
            expireSnapshots(function (err) {
              if (err) {
                return self.emit('snapshot:error', err)
              }

              self.emit('snapshot:complete', backupName)
            })
          })
        })
      })
    })
  }

  var interval = (parseInt(self.opts.interval, 10) || 3600) * 1000
  this._snapshotInterval = setInterval(doSnapshot, interval)

  if (this.opts.noSnapshot === false) {
    setTimeout(doSnapshot, 500)
  }
}

LevelSnapshot.prototype.stop = function () {
  debug('stopping snapshots')
  clearInterval(this._snapshotInterval)
  this._snapshotInterval = null
}

LevelSnapshot.prototype.createSnapshotServer = function () {
  var self = this

  debugs('createSnapshotServer')

  function syncSnapshot (streamServer, syncTime, callback) {
    debugs('syncTime: %s', syncTime)

    fs.readdir(path.join(self.opts.path, 'snapshot-' + syncTime), function (err, files) {
      if (err) return callback(err)
      debugs('snapshots: %j', files)

      streamServer.fileCount({ count: files.length })

      async.eachSeries(files, function (file, cb) {
        debugs('sending file: %s', file)

        var fullPath = path.join(self.opts.path, 'snapshot-' + syncTime, file)

        fs.createReadStream(fullPath)
          .pipe(through2.obj(function (chunk, enc, callback) {
            var data = {
              filename: file,
              contents: chunk,
              ended: false
            }

            callback(null, data)
          }))
          .on('data', function (data) {
            streamServer.file(data)
          })
          .on('end', function (data) {
            streamServer.file({
              filename: file,
              contents: new Buffer('end'),
              ended: true
            })

            debugs('ending file: %s', file)

            cb()
          })
      }, callback)
    })
  }

  function syncLogs (streamServer) {
    streamServer.once('status', function (m) {
      if (m.status === 8) {
        debugs('sending logs now')

        var readStream = through2()
        readStream.on('end', function () {
          console.log('ending!!!')
        })

        fs.createReadStream(self._logStreamCurrentFile)
          .pipe(through2.obj(function (chunk, enc, callback) {
            var self = this

            var lines = chunk.toString().split('\n').filter(function (e) {
              return e !== ''
            })

            lines.forEach(function (line) {
              self.push(line)
            })

            callback()
          }))
          .pipe(readStream, {end: false})

        self._logStream.pipe(readStream, {end: false})

        readStream.on('data', function (data) {
          var d
          try {
            d = JSON.parse(data)
          } catch (e) {
            console.error('failed to parse JSON', data)
            throw e
          }

          streamServer.log({
            type: new Buffer(String(d.type)),
            key: new Buffer(String(d.key)),
            value: new Buffer(String(d.value))
          })
        })
      }
    })
  }

  function checkSnapshotSync (time, callback) {
    async.waterfall([
      function (callback) {
        fs.readdir(self.opts.path, callback)
      },
      function (files, callback) {
        if (files[files.length - 1] === 'snapshot-current') {
          files.pop()
        }

        var latest = files[files.length - 1]

        callback(null, latest)
      },
      function (latest, callback) {
        var latest_time = latest.split('-')[1] || 0

        callback(null, latest_time)
      }
    ], function (err, latest_time) {
      if (err) return callback(err)

      if (time < latest_time) {
        return callback(null, latest_time)
      }

      callback(null, false)
    })
  }

  return net.createServer(function (con) {
    con.on('error', function (err) {
      self.emit('snapshot:error', err)
    })

    var streamServer = self.protocols()
    con.pipe(streamServer).pipe(con)

    streamServer.on('status', function (m) {
      debugs('status received: %j', m)

      if (m.status === 0) {
        checkSnapshotSync(m.time || 0, function (err, doSync) {
          if (err) {
            return console.error('checkSnapshotSync() failed', err)
          }
          if (doSync) {
            syncSnapshot(streamServer, doSync, function (err) {
              if (err) {
                return console.error('syncSnapshot() failed', err)
              }
              syncLogs(streamServer)
            })
          } else {
            syncLogs()
          }
        })
      }
    })
  })
}

LevelSnapshot.prototype.createSnapshotClient = function (port, host) {
  var self = this

  var socket = net.connect(port, host || 'localhost')

  debugc('closing db')
  self.db.close(function () {
    debugc('db closed')
  })

  var lastSnapshotSync = this.getLastSnapshotSyncTime()

  var streamClient = self.protocols()

  socket.pipe(streamClient).pipe(socket)

  var files = {}

  rimraf.sync(self.db.location)
  mkdirp.sync(self.db.location)

  streamClient.status({
    status: 0,
    time: lastSnapshotSync
  })

  var filesFlushed

  streamClient.on('fileCount', function (m) {
    // We can open the db after all files have been flushed
    filesFlushed = after(m.count, function () {
      self.setLastSnapshotSyncTime(String(timestamp()))
      self.db.open(function (err) {
        if (err) {
          throw err
        }
        self.emit('snapshot:db-reopened')
        debugc('db reopened successfully')
        streamClient.status({
          status: 8
        })
      })
    })
  })

  streamClient.on('file', function (m) {
    var filePath = path.join(self.db.location, m.filename)

    if (typeof files[m.filename] === 'undefined') {
      files[m.filename] = fs.createWriteStream(filePath)
    }

    if (m.ended === true) {
      debugc('file written: %s', m.filename)
      var file = files[m.filename]
      file.on('finish', filesFlushed)
      file.end()
      return
    }

    files[m.filename].write(m.contents)
  })

  streamClient.on('log', function (l) {
    debugc('log received: type: %s, key: %s', l.type.toString(), l.key.toString())

    var action = l.type.toString()
    var key = l.key.toString()
    var value = l.value.toString()

    self.db._snapshot[action](key, value, function (err) {
      if (err) {
        console.log('Error writing to database, this is bad')
      }
    })
  })

  return socket
}

LevelSnapshot.prototype.getLastSnapshotSyncTime = function () {
  if (fs.existsSync(this.opts.lastSyncPath)) {
    return Number(fs.readFileSync(this.opts.lastSyncPath, 'utf8'))
  }

  return 0
}

LevelSnapshot.prototype.setLastSnapshotSyncTime = function (time) {
  fs.writeFileSync(this.opts.lastSyncPath, time, 'utf8')
}
