# RocksDB D-Lang

A straightforward binding to RocksDB v5.0.1 in D-lang.

## Building

You need a valid rocksdb library somewhere your linker can find it.

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