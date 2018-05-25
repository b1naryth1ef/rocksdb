# RocksDB D-Lang

A straightforward binding to RocksDB v5.0.1 in D-lang.

## Building

You need a valid rocksdb library in the root of this project so your linker can find it.  It is recommended to use a recent version of rocksdb, tested with 

- facebook-rocksdb-v5.12.4 (https://github.com/facebook/rocksdb/archive/v5.12.4.tar.gz)

Note: Windows rocksdb will work and recommendation is to use vcpkg to build rocks first, and copy your rocksdb-shared.dll into the root of the parent project.

## Testing

> dub test

will launch a benchmark, and should be enough to convine one of the functionality and performance.

## Example

```D
import rocksdb;
import std.conv : to;

auto opts = new DBOptions;
opts.createIfMissing = true;
opts.errorIfExists = false;

auto db = new Database(opts, "testdb");

// Put a value into the database
db.put("key", "value");

// Get a value out
assert(db.get("key") == "value");

// Delete a value
db.remove("key");

// Add values in bulk
auto batch = new WriteBatch;
for (int i = 0; i < 1000; i++) {
  batch.put(i.to!string, i.to!string);
}
db.write(batch);

// Iterate over the DB
auto iter = db.iter();
foreach (key, value; iter) {
  db.remove(key);
}
destroy(iter);

// Close the database
db.close();
```