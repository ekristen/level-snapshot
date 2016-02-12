# level-snapshot

Snapshot Level DB using level-hyper's liveBackup method. After each snapshot a transaction log is kept.

## Usage

```js
var level = require('level-hyper')
var Snapshot = require('level-snapshot')

var db = level('./db')
var snapshot = Snapshot(db)

snapshot.on('snapshot:complete', function (snapshotName) {
  console.log('took snapshot', snapshotName)
})

snapshot.start()
```

## Api

#### `var snapshot = Snapshot(db[, options])`

Create a `level-snapshot` instance using `db`. Options include:

```js
{
  path: './snaphots',        // store snapshots in this folder
  logPath: './logs',         // store transaction logs in this folder
  interval: 3600,            // interval in seconds between snapshots
  lastSyncPath: './lastsync' // file to store last sync time
}
```

#### `snapshot.start()`

Take a snapshot on `nextTick()` and continue taking snapshots at each `interval`. Defaults to one snapshot per hour.


#### `snapshot.stop()`

Stop taking snapshots.


#### `var server = snapshot.createServer()`

Create and return a `tcp` server. The server is not listening to connections until you call `.listen()` on it.


#### `var client = snapshot.createClient(port, host)`

Create a snapshot client and connect it to `port` and `host`.

If the client has never synced with the server before, it will:

* close the db
* stream the latest snapshot from the server
* re-open the db
* receive live updates from the server

If the client has synced before, it will:

* receive all logs since the last sync time
* receive live updates from the server

#### `var time = snapshot.getLastSnapshotSyncTime()`

Returns the last sync time.

#### `snapshot.on('snapshot:start', function (name) {})`

Emitted when a snapshot has started.

#### `snapshot.on('snapshot:complete', function (name) {})`

Emitted when a snapshot has completed successfully.

#### `snapshot.on('snapshot:error', function (err) {})`

Emitted if a snapshot failed.

#### `snapshot.on('snapshot:cleanup', function (path) {})`

Emitted when an old snapshot has been cleaned up.

#### `snapshot.on('snapshot:db-reopened', function () {})`

Emitted when the db was reopened successfully.


## License
MIT
