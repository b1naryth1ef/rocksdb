module rocksdb.database;

import std.conv : to;
import std.file : isDir, exists;
import std.array : array;
import std.string : fromStringz, toStringz;
import std.format : format;

import core.stdc.stdlib : cfree = free;
import core.stdc.string : strlen;

import rocksdb.batch,
       rocksdb.options,
       rocksdb.iterator,
       rocksdb.queryable,
       rocksdb.comparator,
       rocksdb.columnfamily;

extern (C) {
  struct rocksdb_t {};

  void rocksdb_put(rocksdb_t*, const rocksdb_writeoptions_t*, const char*, size_t, const char*, size_t, char**);
  void rocksdb_put_cf(rocksdb_t*, const rocksdb_writeoptions_t*, rocksdb_column_family_handle_t*, const char*, size_t, const char*, size_t, char**);

  char* rocksdb_get(rocksdb_t*, const rocksdb_readoptions_t*, const char*, size_t, size_t*, char**);
  char* rocksdb_get_cf(rocksdb_t*, const rocksdb_readoptions_t*, rocksdb_column_family_handle_t*, const char*, size_t, size_t*, char**);

  void rocksdb_delete(rocksdb_t*, const rocksdb_writeoptions_t*, const char*, size_t, char**);
  void rocksdb_delete_cf(rocksdb_t*, const rocksdb_writeoptions_t*, rocksdb_column_family_handle_t*, const char*, size_t, char**);

  void rocksdb_write(rocksdb_t*, const rocksdb_writeoptions_t*, rocksdb_writebatch_t*, char**);

  rocksdb_t* rocksdb_open(const rocksdb_options_t*, const char*, char**);
  rocksdb_t* rocksdb_open_column_families(const rocksdb_options_t*, const char*, int, const char**, const rocksdb_options_t**, rocksdb_column_family_handle_t**, char**);

  void rocksdb_close(rocksdb_t*);

}

void ensureRocks(char* err) {
  if (err) {
    throw new Exception(format("Error: %s", fromStringz(err)));
  }
}

class Database {
  mixin Getable;
  mixin Putable;
  mixin Removeable;

  rocksdb_t* db;

  DBOptions opts;
  WriteOptions writeOptions;
  ReadOptions readOptions;

  ColumnFamily[string] columnFamilies;

  this(DBOptions opts, string path, DBOptions[string] columnFamilies = null) {
    char* err = null;
    this.opts = opts;

    string[] existingColumnFamilies;

    // If there is an existing database we can check for existing column families
    if (exists(path) && isDir(path)) {
      // First check if the database has any column families
      existingColumnFamilies = Database.listColumnFamilies(opts, path);
    }

    if (columnFamilies || existingColumnFamilies.length >= 1) {
      immutable(char*)[] columnFamilyNames;
      rocksdb_options_t*[] columnFamilyOptions;

      foreach (k; existingColumnFamilies) {
        columnFamilyNames ~= toStringz(k);

        if ((k in columnFamilies) !is null) {
          columnFamilyOptions ~= columnFamilies[k].opts;
        } else {
          columnFamilyOptions ~= opts.opts;
        }
      }

      rocksdb_column_family_handle_t*[] result;
      result.length = columnFamilyNames.length;

      this.db = rocksdb_open_column_families(
        opts.opts,
        toStringz(path),
        cast(int)columnFamilyNames.length,
        columnFamilyNames.ptr,
        columnFamilyOptions.ptr,
        result.ptr,
        &err);

      foreach (idx, handle; result) {
        this.columnFamilies[existingColumnFamilies[idx]] = new ColumnFamily(
          this,
          existingColumnFamilies[idx],
          handle,
        );
      }
    } else {
      this.db = rocksdb_open(opts.opts, toStringz(path), &err);
    }

    err.ensureRocks();

    this.writeOptions = new WriteOptions;
    this.readOptions = new ReadOptions;
  }

  ~this() {
    if (this.db) {
      foreach (k, v; this.columnFamilies) {
        rocksdb_column_family_handle_destroy(v.cf);
      }

      rocksdb_close(this.db);
    }
  }

  ColumnFamily createColumnFamily(string name, DBOptions opts = null) {
    char* err = null;

    auto cfh = rocksdb_create_column_family(this.db, (opts ? opts : this.opts).opts, toStringz(name), &err);
    err.ensureRocks();

    this.columnFamilies[name] = new ColumnFamily(this, name, cfh);
    return this.columnFamilies[name];
  }

  static string[] listColumnFamilies(DBOptions opts, string path) {
    char* err = null;
    size_t numColumnFamilies;

    char** columnFamilies = rocksdb_list_column_families(
      opts.opts,
      toStringz(path),
      &numColumnFamilies,
      &err);

    err.ensureRocks();

    string[] result = new string[](numColumnFamilies);

    // Iterate over and convert/copy all column family names
    for (size_t i = 0; i < numColumnFamilies; i++) {
      result[i] = fromStringz(columnFamilies[i]).to!string;
    }

    rocksdb_list_column_families_destroy(columnFamilies, numColumnFamilies);

    return result;
  }

  byte[] getImpl(byte[] key, ColumnFamily family, ReadOptions opts = null) {
    size_t len;
    char* err;
    byte* value;

    if (family) {
      value = cast(byte*)rocksdb_get_cf(
        this.db,
        (opts ? opts : this.readOptions).opts,
        family.cf,
        cast(char*)key.ptr,
        key.length,
        &len,
        &err);
    } else {
      value = cast(byte*)rocksdb_get(
        this.db,
        (opts ? opts : this.readOptions).opts,
        cast(char*)key.ptr,
        key.length,
        &len,
        &err);
    }

    err.ensureRocks();

    byte[] result = (value[0..len]).array;
    cfree(value);
    return result;
  }

  void putImpl(byte[] key, byte[] value, ColumnFamily family, WriteOptions opts = null) {
    char* err;

    if (family) {
      rocksdb_put_cf(this.db,
        (opts ? opts : this.writeOptions).opts,
        family.cf,
        cast(char*)key.ptr, key.length,
        cast(char*)value.ptr, value.length,
        &err);
    } else {
      rocksdb_put(this.db,
        (opts ? opts : this.writeOptions).opts,
        cast(char*)key.ptr, key.length,
        cast(char*)value.ptr, value.length,
        &err);
    }

    err.ensureRocks();
  }

  void removeImpl(byte[] key, ColumnFamily family, WriteOptions opts = null) {
    char* err;

    if (family) {
      rocksdb_delete_cf(
        this.db,
        (opts ? opts : this.writeOptions).opts,
        family.cf,
        cast(char*)key.ptr,
        key.length,
        &err);
    } else {
      rocksdb_delete(
        this.db,
        (opts ? opts : this.writeOptions).opts,
        cast(char*)key.ptr,
        key.length,
        &err);
    }

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

  void close() {
    destroy(this);
  }
}

unittest {
  import std.stdio : writefln;
  import std.datetime : benchmark;

  writefln("Testing Database");

  auto opts = new DBOptions;
  opts.createIfMissing = true;
  opts.errorIfExists = false;
  opts.compression = CompressionType.NONE;

  auto db = new Database(opts, "test");

  // Test putting and getting into a family
  auto family = db.columnFamilies["test1"];
  family.put("key", "notvalue");
  assert(family.get("key") == "notvalue");
  assert(db.get("key") != "notvalue");

  // Test string putting and getting
  db.put("key", "value");
  assert(db.get("key") == "value");
  db.put("key", "value2");
  assert(db.get("key") == "value2");

  byte[] key = ['\x00', '\x00'];
  byte[] value = ['\x01', '\x02'];

  // Test byte based putting / getting
  db.put(key, value);
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
  writefln("  writing a value 100000 times: %sms", writeRes[0].msecs);

  auto readRes = benchmark!(() => readBench(100_000))(1);
  writefln("  reading a value 100000 times: %sms", readRes[0].msecs);

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
  writefln("  batch writing 100000 values: %sms", writeBatchRes[0].msecs);
  readBench(100_000);

  // Test scanning from a location
  bool found = false;
  auto iterFrom = db.iter();
  iterFrom.seek("key");
  foreach (key, value; iterFrom) {
    assert(value == "value2");
    assert(!found);
    found = true;
  }
  iterFrom.close();
  assert(found);

  found = false;
  int keyCount = 0;
  auto iter = db.iter();

  foreach (key, value; iter) {
    if (key == "key") {
      assert(value == "value2");
      found = true;
    }
    keyCount++;
  }
  iter.close();
  assert(found);
  assert(keyCount == 100001);
  destroy(db);
}
