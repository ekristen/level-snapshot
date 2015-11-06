var fs = require('fs')
var path = require('path')
var util = require('util')
var events = require('events')

var after = require('after')
var xtend = require('xtend')
var mkdirp = require('mkdirp')
var rimraf = require('rimraf')
var through2 = require('through2')

var LevelSnapshot = module.exports = function(db, opts) {
  if (db._snapshot) {
    return
  }

  if (!(this instanceof LevelSnapshot)) return new LevelSnapshot(db, opts)

  this.opts = xtend({
    path: './db',
    log: {
      path: './logs'
    },
    snapshot: {
      path: './snapshots',
      interval: 3600
    }
  })

  this.db = db
  this._snapshotInterval = null

  this._logStream = through2()
  this._logStreamPrevious = null
  this._logStreamCurrent = null


  mkdirp.sync(this.opts.snapshot.path)
  mkdirp.sync(this.opts.log.path)

  var self = this
  
  this.setupEvents()

  this.attach()
  this.snapshot()

  return this
}
util.inherits(LevelSnapshot, events.EventEmitter)

LevelSnapshot.prototype.setupEvents = function() {
  var self = this

  this.on('snapshot:start', function(snapshotName) {
    self.roll(snapshotName)
  })
  
  this.on('snapshot:error', function(err) {
    console.log('snapshot:error', err)
  })
  
  this.on('snapshot:cleanup', function(snapshotName) {
    fs.unlink(path.join(self.opts.log.path, snapshotName), function(err) {})
  })
}

LevelSnapshot.prototype.roll = function(snapshotName) {
  var self = this

  var filePath = path.join(this.opts.log.path, snapshotName)

  if (this._logStreamCurrent == null) {
    this._logStreamCurrentFile = filePath
    this._logStreamPreviousFile = filePath
    this._logStreamCurrent = fs.createWriteStream(filePath)
    this._logStreamPrevious = this._logStreamCurrent
    
    this._logStream.pipe(this._logStreamCurrent)
  } else {
    this._logStreamCurrentFile = filePath
    this._logStreamCurrent = fs.createWriteStream(filePath)

    this._logStream.pipe(this._logStreamCurrent)
    this._logStream.unpipe(this._logStreamPrevious)

    this._logStreamPrevious.close()
    this._logStreamPrevious = this._logStreamCurrent
  }
  
}

LevelSnapshot.prototype.attach = function() {
  var self = this

  function createLogStream() {
    var readStream = through2()
    fs.createReadStream(this._logStreamCurrentFile).pipe(readStream, {end: false})
    self._logStream.pipe(readStream, {end: false})
    return readStream
  }

  function put(db, key, value, options, callback) {
    if (typeof options == 'function') {
      callback = options
      options = {}
    }

    self._logStream.write(new Buffer(JSON.stringify({type: 'put', key: key, value: value, options: options})) + "\n")

    self.db._snapshot.put.call(db, key, value, options, callback)
  }
  
  function del(db, key, options, callback) {
    if (typeof options == 'function') {
      callback = options
      options = {}
    }

    self._logStream.write(new Buffer(JSON.stringify({type: 'del', key: key, value: value, options: options})) + "\n")

    self.db._snapshot.del.call(db, key, options, callback)
  }

  this.db._snapshot = {
    put: this.db.put.bind(this.db),
    del: this.db.del.bind(this.db),
    createLogStream: createLogStream.bind(this)
  }

  this.db.put = put.bind(null, this.db)
  this.db.del = del.bind(null, this.db)
}

LevelSnapshot.prototype.snapshot = function() {
  var self = this

  function expireSnapshots(callback) {
    fs.readdir(self.opts.snapshot.path, function(err, files) {
      var done = after(files.length, callback)

      var len = files.length - 4
      for (var i=0; i<len; i++) {
        var filePath = path.join(self.opts.snapshot.path, files[i])
        rimraf(filePath, function(err) {
          if (err) {
            return done(err)
          }
          
          self.emit('snapshot:cleanup', files[i])
          done()
        })
      }
    })
  }

  function doSnapshot() {
    var backupName = ['snapshot', new Date().getTime()].join('-')
    var backupPath = path.join(self.opts.path, ['backup', backupName].join('-'))

    self.emit('snapshot:start', backupName)

    self.db.liveBackup(backupName, function() {
      var snapshotPath = path.join(self.opts.snapshot.path, backupName)
      var currentPath = path.join(self.opts.snapshot.path, 'snapshot-current')

      // Move Backup to Snapshot Directory
      fs.rename(backupPath, snapshotPath, function(err) {
        if (err) {
          return self.emit('snapshot:error', err)
        }

        // Remove Link to Old Snapshot
        fs.unlink(currentPath, function(err) {
          if (err && err.code != 'ENOENT') {
            return self.emit('snapshot:error', err)
          }

          // Link 
          fs.symlink(path.basename(snapshotPath), currentPath, function(err) {
            if (err) {
              return self.emit('snapshot:error', err)
            }

            // Expire
            expireSnapshots(function(err) {
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
  
  this._snapshotInterval = setInterval(doSnapshot, (parseInt(self.opts.snapshot.interval) || 3600) * 1000)

  doSnapshot()
}


