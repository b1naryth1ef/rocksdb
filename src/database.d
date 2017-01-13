module rocksdb.database;

import std.array : array;
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
  void rocksdb_delete(rocksdb_t*, const rocksdb_writeoptions_t*, const char*, size_t, char**);

  void rocksdb_write(rocksdb_t*, const rocksdb_writeoptions_t*, rocksdb_writebatch_t*, char**);

  rocksdb_t* rocksdb_open(const rocksdb_options_t*, const char* name, char** errptr);
  void rocksdb_close(rocksdb_t*);
}

void ensureRocks(char* err) {
  if (err) {
    throw new Exception(format("Error: %s", fromStringz(err)));
  }
}

class Database {
  rocksdb_t* db;

  WriteOptions writeOptions;
  ReadOptions readOptions;

  this(DBOptions opts, string path) {
    char *err = null;
    this.db = rocksdb_open(opts.opts, toStringz(path), &err);
    err.ensureRocks();
    
    this.writeOptions = new WriteOptions;
    this.readOptions = new ReadOptions;
  }

  ~this() {
    if (this.db) {
      rocksdb_close(this.db);
    }
  }

  string get(string key, ReadOptions opts=null) {
    return (cast(char[])this.get(cast(byte[])key, opts)).to!string;
  }

  byte[] get(byte[] key, ReadOptions opts = null) {
    size_t len;
    char* err;
    byte* value = cast(byte*)rocksdb_get(
      this.db, (opts ? opts : this.readOptions).opts, cast(char*)key.ptr, key.length, &len, &err);
    err.ensureRocks();

    byte[] result = (value[0..len]).array;
    cfree(value);
    return result;
  }

  void put(string key, string value, WriteOptions opts = null) {
    this.put(cast(byte[])key, cast(byte[])value, opts);
  }

  void put(byte[] key, byte[] value, WriteOptions opts = null) {
    char* err;
    rocksdb_put(this.db,
      (opts ? opts : this.writeOptions).opts,
      cast(char*)key.ptr, key.length,
      cast(char*)value.ptr, value.length,
      &err);
    err.ensureRocks();
  }

  void remove(string key, WriteOptions opts = null) {
    this.remove(cast(byte[])key, opts);
  }

  void remove(byte[] key, WriteOptions opts = null) {
    char* err;
    rocksdb_delete(this.db, (opts ? opts : this.writeOptions).opts, cast(char*)key.ptr, key.length, &err);
    err.ensureRocks();
  }

  void write(WriteBatch batch, WriteOptions opts = null) {
    char* err;
    rocksdb_write(this.db, (opts ? opts : this.writeOptions).opts, batch.batch, &err);
    err.ensureRocks();
  }

  Iterator iter(ReadOptions opts = null) {
    return new Iterator(this, opts ? opts : this.readOptions);
  }
}

unittest {
  import std.stdio : writefln;
  import std.datetime : benchmark;

  auto opts = new DBOptions;
  opts.createIfMissing = true;
  opts.errorIfExists = false;
  opts.compression = CompressionType.NONE;
  opts.enableStatistics();

  auto db = new Database(opts, "test");

  // Test string putting and getting
  db.put("key", "value");
  writefln("%s", db.get("key"));
  assert(db.get("key") == "value");
  db.put("key", "value2");
  assert(db.get("key") == "value2");

  byte[] key = ['\x00', '\x00'];
  byte[] value = ['\x01', '\x02'];

  // Test byte based putting / getting
  db.put(key, value);
  writefln("%s", db.get(key));
  assert(db.get(key) == value);
  db.remove(key);

  // Benchmarks

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
    // writefln("%s", key);
  }
  destroy(iter);

  assert(found);

  writefln("Keys: %s", keyCount);
  assert(keyCount == 100001);

  destroy(db);
}