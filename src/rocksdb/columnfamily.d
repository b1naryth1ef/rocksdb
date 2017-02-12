module rocksdb.columnfamily;

import rocksdb.options : rocksdb_options_t, ReadOptions, WriteOptions;
import rocksdb.iterator : Iterator;
import rocksdb.database : Database, rocksdb_t, ensureRocks;
import rocksdb.queryable : Getable, Putable, Removeable;

extern (C) {
  struct rocksdb_column_family_handle_t {};

  char** rocksdb_list_column_families(const rocksdb_options_t*, const char*, size_t*, char**);
  void rocksdb_list_column_families_destroy(char**, size_t);

  rocksdb_column_family_handle_t* rocksdb_create_column_family(rocksdb_t*, const rocksdb_options_t*, const char*, char**);
  void rocksdb_drop_column_family(rocksdb_t*, rocksdb_column_family_handle_t*, char**);
  void rocksdb_column_family_handle_destroy(rocksdb_column_family_handle_t*);
}

class ColumnFamily {
  mixin Getable;
  mixin Putable;
  mixin Removeable;

  Database db;
  string name;
  rocksdb_column_family_handle_t* cf;

  this(Database db, string name, rocksdb_column_family_handle_t* cf) {
    this.db = db;
    this.name = name;
    this.cf = cf;
  }

  Iterator iter(ReadOptions opts = null) {
    return new Iterator(this.db, this, opts ? opts : this.db.readOptions);
  }

  void withIter(void delegate(Iterator) dg, ReadOptions opts = null) {
    Iterator iter = this.iter(opts);
    scope (exit) destroy(iter);
    dg(iter);
  }

  void drop() {
    char* err = null;
    rocksdb_drop_column_family(this.db.db, this.cf, &err);
    err.ensureRocks();
  }

  ubyte[] getImpl(ubyte[] key, ColumnFamily family, ReadOptions opts = null) {
    assert(family == this || family is null);
    return this.db.getImpl(key, this, opts);
  }

  void putImpl(ubyte[] key, ubyte[] value, ColumnFamily family, WriteOptions opts = null) {
    assert(family == this || family is null);
    this.db.putImpl(key, value, this, opts);
  }

  void removeImpl(ubyte[] key, ColumnFamily family, WriteOptions opts = null) {
    assert(family == this || family is null);
    this.db.removeImpl(key, this, opts);
  }
}

unittest {
  import std.stdio : writefln;
  import std.datetime : benchmark;
  import std.conv : to;
  import std.algorithm.searching : startsWith;
  import rocksdb.options : DBOptions, CompressionType;

  writefln("Testing Column Families");

  // DB Options
  auto opts = new DBOptions;
  opts.createIfMissing = true;
  opts.errorIfExists = false;
  opts.compression = CompressionType.NONE;

  // Create the database (if it does not exist)
  auto db = new Database(opts, "test");

  string[] columnFamilies = [
    "test",
    "test1",
    "test2",
    "test3",
    "test4",
    "wow",
  ];

  // create a bunch of column families
  foreach (cf; columnFamilies) {
    if ((cf in db.columnFamilies) is null) {
      db.createColumnFamily(cf);
    }
  }

  db.close();
  db = new Database(opts, "test");
  scope (exit) destroy(db);

  // Test column family listing
  assert(Database.listColumnFamilies(opts, "test").length == columnFamilies.length + 1);

  void testColumnFamily(ColumnFamily cf, int times) {
    for (int i = 0; i < times; i++) {
      cf.putString(cf.name ~ i.to!string, i.to!string);
    }

    for (int i = 0; i < times; i++) {
      assert(cf.getString(cf.name ~ i.to!string) == i.to!string);
    }

    cf.withIter((iter) {
      foreach (key, value; iter) {
        assert(key.startsWith(cf.name));
      }
    });
  }

  foreach (name, cf; db.columnFamilies) {
    if (name == "default") continue;

    writefln("  %s", name);
    testColumnFamily(cf, 1000);
  }
}
