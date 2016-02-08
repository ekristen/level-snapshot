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
var split = require('split')
var Multistream = require('multistream')
var pumpify = require('pumpify')
var timestamp = require('monotonic-timestamp')
var protobufs = require('protocol-buffers-stream')

var debug = require('debug')('level-snapshot')
var debugc = require('debug')('level-snapshot:client')
var debugs = require('debug')('level-snapshot:server')

var statuses = {
  0: 'SYNC_SNAPSHOT', 'SYNC_SNAPSHOT': 0,
  8: 'SYNC_LOGS', 'SYNC_LOGS': 8
}

var LevelSnapshot = module.exports = function (db, opts) {
  if (db._snapshot) return

  if (typeof db.db.liveBackup !== 'function') {
    throw new Error('level-snapshot requires level-hyper')
  }

  if (!(this instanceof LevelSnapshot)) {
    return new LevelSnapshot(db, opts)
  }

  this.opts = xtend({
    path: './snapshots',
    logPath: './logs',
    interval: 3600,
    lastSyncPath: './lastsync'
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

  this.setupEvents()
  this.attach()

  return this
}
util.inherits(LevelSnapshot, events.EventEmitter)

LevelSnapshot.prototype.setupEvents = function () {
  var self = this
  this.on('snapshot:start', function (name) {
    debug('taking snapshot %s', name)
    self.roll(snapshotName)
  })
  this.on('snapshot:complete', function (name) {
    debug('snapshot %s completed successfully', name)
  })
  this.on('snapshot:error', function (err) {
    console.error('failed taking snapshot', err)
  })
  this.on('snapshot:cleanup', function (name) {
    var fileName = path.join(self.opts.logPath, name)
    fs.unlink(fileName, function (err) {
      if (err) console.log('failed to remove', fileName)
    })
  })
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

  this.db._snapshot = {
    put: this.db.put.bind(this.db),
    del: this.db.del.bind(this.db),
    batch: this.db.batch.bind(this.db)
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

  function batch (db, ops, options, callback) {
    if (typeof options === 'function') {
      callback = options
      options = {}
    }
    ops.forEach(function (op) {
      op.options = op.options || options
      write(op)
    })
    self.db._snapshot.batch.call(db, ops, options, callback)
  }

  function write (data) {
    self._logStream.write(JSON.stringify(data) + '\n')
  }

  this.db.put = put.bind(null, this.db)
  this.db.del = del.bind(null, this.db)
  this.db.batch = batch.bind(null, this.db)
}

LevelSnapshot.prototype.start = function () {
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

  if (this._snapshotInterval === null) {
    var interval = (parseInt(self.opts.interval, 10) || 3600) * 1000
    debug('starting snapshots at interval %d', interval)
    this._snapshotInterval = setInterval(doSnapshot, interval)
  } else {
    debug('snapshots already started')
  }

  debug('triggering doSnapshot() on next tick')
  process.nextTick(doSnapshot)
}

LevelSnapshot.prototype.stop = function () {
  debug('stopping snapshots')
  clearInterval(this._snapshotInterval)
  this._snapshotInterval = null
}

LevelSnapshot.prototype.createServer = function () {
  debugs('createServer')

  var self = this

  function syncSnapshot (streamServer, folder, callback) {
    debugs('syncSnapshot: %s', folder)

    var ts = folder.split('-')[1]
    var fullFolderPath = path.join(self.opts.path, folder)

    fs.readdir(fullFolderPath, function (err, files) {
      if (err) return callback(err)

      debugs('snapshots: %j', files)
      streamServer.fileCount({ count: files.length, timestamp: ts })

      async.eachSeries(files, function (file, nextFile) {
        debugs('sending file: %s', file)

        var fullPath = path.join(fullFolderPath, file)

        fs.createReadStream(fullPath)
          .pipe(through2.obj(function (chunk, enc, done) {
            done(null, { filename: file, contents: chunk })
          }))
          .on('data', function (data) {
            streamServer.file(data)
          })
          .on('end', function (data) {
            streamServer.file({ filename: file, ended: true })
            debugs('ending file: %s', file)
            nextFile()
          })
      }, callback)
    })
  }

  function syncLogs (streamServer, since) {
    debugs('sending logs now')

    var logPath = self.opts.logPath
    getLogs(logPath, since, function (err, logs) {
      debugs('streaming logs %j', logs)

      var sink = through2()
      if (logs.length) {
        var logStreams = logs.map(function (log) {
          var fullPath = path.join(logPath, log)
          return fs.createReadStream(fullPath)
        })
        Multistream(logStreams).pipe(split()).pipe(sink, { end: false })
      }

      self._logStream.pipe(sink, { end: false })

      sink.on('data', function (data) {
        try {
          var d = JSON.parse(data)
          streamServer.log({
            type: new Buffer(String(d.type)),
            key: new Buffer(String(d.key)),
            value: new Buffer(String(d.value))
          })
        } catch (e) {
          console.error('failed to parse JSON', data.toString())
        }
      })
    })
  }

  function getLogs (logPath, since, cb) {
    fs.readdir(logPath, function (err, files) {
      if (err) return cb(err)
      if (!files.length) return cb(null, [])
      if (since === -1) {
        return cb(null, [ files[files.length - 1] ])
      }
      cb(null, files.filter(function (file) {
        return since < Number(file.split('-')[1])
      }))
    })
  }

  function currentSnapshotFolder (cb) {
    fs.readlink(path.join(self.opts.path, 'snapshot-current'), cb)
  }

  return net.createServer(function (con) {
    con.on('error', function (err) {
      self.emit('snapshot:error', err)
    })

    var streamServer = self.protocols()
    con.pipe(streamServer).pipe(con)

    streamServer.on('status', function (m) {
      debugs('status received: %j', m)
      var status = statuses[m.status]
      if (status === 'SYNC_SNAPSHOT') {
        currentSnapshotFolder(function (err, folder) {
          if (err) return console.error('currentSnapshotFolder() failed', err)
          syncSnapshot(streamServer, folder, function (err) {
            if (err) return console.error('syncSnapshot() failed', err)
          })
        })
      } else if (status === 'SYNC_LOGS') {
        syncLogs(streamServer, Number(m.time || -1))
      } else {
        debugs('unknown status')
      }
    })
  })
}

LevelSnapshot.prototype.createClient = function (port, host) {
  debugc('createClient')

  var self = this
  var socket = net.connect(port, host || 'localhost')
  var lastSnapshotSync = this.getLastSnapshotSyncTime()
  var streamClient = self.protocols()

  socket.pipe(streamClient).pipe(socket)

  function sendStatus (status, time) {
    var data = { status: statuses[status], time: time || 0 }
    debugc('client sending status %j', data)
    streamClient.status(data)
  }

  streamClient.on('log', function (l) {
    debugc('log received: type: %s, key: %s', l.type.toString(), l.key.toString())
    var action = l.type.toString()
    var key = l.key.toString()
    var value = l.value.toString()
    self.db._snapshot[action](key, value, function (err) {
      if (err) console.error('Error writing to database, this is bad')
    })
  })

  if (lastSnapshotSync === 0) {
    var files = {}
    var filesFlushed = function () {}

    debugc('closing db')
    self.db.close(function () {
      debugc('db closed')
    })

    rimraf.sync(self.db.location)
    mkdirp.sync(self.db.location)

    streamClient.once('fileCount', function (m) {
      // We can open the db after all files have been flushed
      debugc('fileCount message %j', m)
      var ts = m.timestamp || timestamp()
      filesFlushed = after(m.count, function () {
        files = {}
        self.setLastSnapshotSyncTime(ts)
        self.db.open(function (err) {
          if (err) throw err
          debugc('db reopened successfully')
          self.emit('snapshot:db-reopened')
          sendStatus('SYNC_LOGS')
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
      } else {
        files[m.filename].write(m.contents)
      }
    })

    sendStatus('SYNC_SNAPSHOT')
  } else {
    sendStatus('SYNC_LOGS', lastSnapshotSync)
  }

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
