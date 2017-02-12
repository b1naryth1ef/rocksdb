module rocksdb.database;

import std.conv : to;
import std.file : isDir, exists;
import std.array : array;
import std.string : fromStringz, toStringz;
import std.format : format;

import core.memory : GC;
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

  void rocksdb_multi_get(rocksdb_t*, const rocksdb_readoptions_t*, size_t, const char**, const size_t*, char**, size_t*, char**);
  void rocksdb_multi_get_cf(rocksdb_t*, const rocksdb_readoptions_t*, const rocksdb_column_family_handle_t*, size_t, const char**, const size_t*, char**, size_t*, char**);

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

  ubyte[] getImpl(ubyte[] key, ColumnFamily family, ReadOptions opts = null) {
    size_t len;
    char* err;
    ubyte* value;

    if (family) {
      value = cast(ubyte*)rocksdb_get_cf(
        this.db,
        (opts ? opts : this.readOptions).opts,
        family.cf,
        cast(char*)key.ptr,
        key.length,
        &len,
        &err);
    } else {
      value = cast(ubyte*)rocksdb_get(
        this.db,
        (opts ? opts : this.readOptions).opts,
        cast(char*)key.ptr,
        key.length,
        &len,
        &err);
    }

    err.ensureRocks();
    GC.addRange(value, len);
    return cast(ubyte[])value[0..len];
  }

  void putImpl(ubyte[] key, ubyte[] value, ColumnFamily family, WriteOptions opts = null) {
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

  void removeImpl(ubyte[] key, ColumnFamily family, WriteOptions opts = null) {
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

  ubyte[][] multiGet(ubyte[][] keys, ColumnFamily family = null, ReadOptions opts = null) {
    char*[] ckeys = new char*[](keys.length);
    size_t[] ckeysSizes = new size_t[](keys.length);

    foreach (idx, key; keys) {
      ckeys[idx] = cast(char*)key;
      ckeysSizes[idx] = key.length;
    }

    char*[] vals = new char*[](keys.length);
    size_t[] valsSizes = new size_t[](keys.length);
    char*[] errs = new char*[](keys.length);

    if (family) {
      rocksdb_multi_get_cf(
        this.db,
        (opts ? opts : this.readOptions).opts,
        family.cf,
        keys.length,
        ckeys.ptr,
        ckeysSizes.ptr,
        vals.ptr,
        valsSizes.ptr,
        errs.ptr);
    } else {
      rocksdb_multi_get(
        this.db,
        (opts ? opts : this.readOptions).opts,
        keys.length,
        ckeys.ptr,
        ckeysSizes.ptr,
        vals.ptr,
        valsSizes.ptr,
        errs.ptr);
    }

    ubyte[][] result = new ubyte[][](keys.length);
    for (int idx = 0; idx < ckeys.length; idx++) {
      errs[idx].ensureRocks();
      result[idx] = cast(ubyte[])vals[idx][0..valsSizes[idx]];
    }

    return result;
  }

  string[] multiGetString(string[] keys, ColumnFamily family = null, ReadOptions opts = null) {
    char*[] ckeys = new char*[](keys.length);
    size_t[] ckeysSizes = new size_t[](keys.length);

    foreach (idx, key; keys) {
      ckeys[idx] = cast(char*)key.ptr;
      ckeysSizes[idx] = key.length;
    }

    char*[] vals = new char*[](keys.length);
    size_t[] valsSizes = new size_t[](keys.length);
    char*[] errs = new char*[](keys.length);

    if (family) {
      rocksdb_multi_get_cf(
        this.db,
        (opts ? opts : this.readOptions).opts,
        family.cf,
        keys.length,
        ckeys.ptr,
        ckeysSizes.ptr,
        vals.ptr,
        valsSizes.ptr,
        errs.ptr);
    } else {
      rocksdb_multi_get(
        this.db,
        (opts ? opts : this.readOptions).opts,
        keys.length,
        ckeys.ptr,
        ckeysSizes.ptr,
        vals.ptr,
        valsSizes.ptr,
        errs.ptr);
    }

    string[] result = new string[](keys.length);
    for (int idx = 0; idx < ckeys.length; idx++) {
      errs[idx].ensureRocks();
      result[idx] = cast(string)vals[idx][0..valsSizes[idx]];
    }

    return result;
  }

  void write(WriteBatch batch, WriteOptions opts = null) {
    char* err;
    rocksdb_write(this.db, (opts ? opts : this.writeOptions).opts, batch.batch, &err);
    err.ensureRocks();
  }

  Iterator iter(ReadOptions opts = null) {
    return new Iterator(this, opts ? opts : this.readOptions);
  }

  void withIter(void delegate(Iterator) dg, ReadOptions opts = null) {
    Iterator iter = this.iter(opts);
    scope (exit) destroy(iter);
    dg(iter);
  }

  void withBatch(void delegate(WriteBatch) dg, WriteOptions opts = null) {
    WriteBatch batch = new WriteBatch;
    scope (exit) destroy(batch);
    scope (success) this.write(batch, opts);
    dg(batch);
  }

  void close() {
    destroy(this);
  }
}

unittest {
  import std.stdio : writefln;
  import std.datetime : benchmark;
  import rocksdb.env : Env;

  writefln("Testing Database");

  auto env = new Env;
  env.backgroundThreads = 2;
  env.highPriorityBackgroundThreads = 1;

  auto opts = new DBOptions;
  opts.createIfMissing = true;
  opts.errorIfExists = false;
  opts.compression = CompressionType.NONE;
  opts.env = env;

  auto db = new Database(opts, "test");

  // Test string putting and getting
  db.putString("key", "value");
  assert(db.getString("key") == "value");
  db.putString("key", "value2");
  assert(db.getString("key") == "value2");

  ubyte[] key = ['\x00', '\x00'];
  ubyte[] value = ['\x01', '\x02'];

  // Test byte based putting / getting
  db.put(key, value);
  assert(db.get(key) == value);
  db.remove(key);

  // Benchmarks

  void writeBench(int times) {
    for (int i = 0; i < times; i++) {
      db.putString(i.to!string, i.to!string);
    }
  }

  void readBench(int times) {
    for (int i = 0; i < times; i++) {
      assert(db.getString(i.to!string) == i.to!string);
    }
  }

  auto writeRes = benchmark!(() => writeBench(100_000))(1);
  writefln("  writing a value 100000 times: %sms", writeRes[0].msecs);

  auto readRes = benchmark!(() => readBench(100_000))(1);
  writefln("  reading a value 100000 times: %sms", readRes[0].msecs);

  // Test batch
  void writeBatchBench(int times) {
    db.withBatch((batch) {
      for (int i = 0; i < times; i++) {
        batch.putString(i.to!string, i.to!string);
      }

      assert(batch.count() == times);
    });
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
