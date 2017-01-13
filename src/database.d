module rocksdb.database;

import std.stdio : writefln;
import std.conv : to;
import std.string : fromStringz, toStringz;
import std.format : format;
import core.stdc.stdlib : cfree = free;
import core.stdc.string : strlen;

import rocksdb.batch,
       rocksdb.options,
       rocksdb.iterator,
       rocksdb.comparator;

extern (C) {
  struct rocksdb_t {};

  void rocksdb_put(rocksdb_t*, const rocksdb_writeoptions_t*, const char*, size_t, const char*, size_t, char**);
  char* rocksdb_get(rocksdb_t*, const rocksdb_readoptions_t*, const char*, size_t, size_t*, char**);

  void rocksdb_write(rocksdb_t*, const rocksdb_writeoptions_t*, rocksdb_writebatch_t*, char**);

  rocksdb_t* rocksdb_open(const rocksdb_options_t*, const char* name, char** errptr);
  void rocksdb_close(rocksdb_t*);
}

class Database {
  rocksdb_t* db;

  WriteOptions writeOptions;
  ReadOptions readOptions;

  this(DBOptions opts, string path) {
    char *err = null;
    this.db = rocksdb_open(opts.opts, toStringz(path), &err);

    if (err) {
      throw new Exception(format("Failed to open rocksdb: %s", fromStringz(err)));
    }
    
    this.writeOptions = new WriteOptions;
    this.readOptions = new ReadOptions;
  }

  ~this() {
    if (this.db) {
      rocksdb_close(this.db);
    }
  }

  string get(string key, ReadOptions opts=null) {
    size_t len;
    immutable char* ckey = toStringz(key);

    char* err;
    char* value = rocksdb_get(this.db, (opts ? opts : this.readOptions).opts, ckey, key.length, &len, &err);

    if (err) {
      throw new Exception(format("Failed to get: %s", fromStringz(err)));
    }

    string result = (value[0..len]).to!string;

    // string result = fromStringz(value).to!string;
    cfree(value);

    return result;
  }

  void put(string key, string value, WriteOptions opts = null) {
    immutable char* ckey = toStringz(key);
    immutable char* cvalue = toStringz(value);

    char* err;
    rocksdb_put(this.db, (opts ? opts : this.writeOptions).opts, ckey, key.length, cvalue, value.length, &err);

    if (err) {
      throw new Exception(format("Failed to put: %s", fromStringz(err)));
    }
  }

  void write(WriteBatch batch, WriteOptions opts = null) {
    char* err;
    rocksdb_write(this.db, (opts ? opts : this.writeOptions).opts, batch.batch, &err);
    if (err) {
      throw new Exception(format("Failed to write: %s", fromStringz(err)));
    }
  }

  Iterator iter(ReadOptions opts = null) {
    return new Iterator(this, opts ? opts : this.readOptions);
  }
}

unittest {
  import std.stdio : writefln;

  auto opts = new DBOptions;
  opts.createIfMissing = true;
  opts.errorIfExists = false;
  opts.compression = CompressionType.NONE;
  opts.enableStatistics();

  auto db = new Database(opts, "test");
  db.put("key", "value");

  assert(db.get("key") == "value");
  db.put("key", "value2");
  assert(db.get("key") == "value2");

  // Benchmarks
  import std.datetime : benchmark;

  void writeBench(int times) {
    for (int i = 0; i < times; i++) {
      db.put(i.to!string, i.to!string);
    }
  }

  void readBench(int times) {
    for (int i = 0; i < times; i++) {
      assert(db.get(i.to!string) == i.to!string);
    }
  }

  auto writeRes = benchmark!(() => writeBench(100_000))(1);
  writefln("Writing a value 100000 times: %sms", writeRes[0].msecs);

  auto readRes = benchmark!(() => readBench(100_000))(1);
  writefln("Reading a value 100000 times: %sms", readRes[0].msecs);

  // Test batch
  auto batch = new WriteBatch;

  void writeBatchBench(int times) {
    auto batch = new WriteBatch;

    for (int i = 0; i < times; i++) {
      batch.put(i.to!string, i.to!string);
    }

    assert(batch.count() == times);
    db.write(batch);
  }

  auto writeBatchRes = benchmark!(() => writeBatchBench(100_000))(1);
  writefln("Batch writing 100000 values: %sms", writeBatchRes[0].msecs);
  readBench(100_000);

  writefln("%s", opts.getStatisticsString());

  bool found = false;
  int keyCount = 0;
  auto iter = db.iter();

  foreach (key, value; iter) {
    if (key == "key") {
      assert(value == "value2");
      found = true;
    }
    keyCount++;
  }
  destroy(iter);

  assert(found);

  writefln("Keys: %s", keyCount);
  assert(keyCount == 100001);

  destroy(db);
}