module rocksdb.columnfamily;

import rocksdb.database : Database, rocksdb_t, ensureRocks;
import rocksdb.options : rocksdb_options_t, ReadOptions, WriteOptions;
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

  void drop() {
    char* err = null;
    rocksdb_drop_column_family(this.db.db, this.cf, &err);
    err.ensureRocks();
  }

  byte[] getImpl(byte[] key, ColumnFamily family, ReadOptions opts = null) {
    assert(family == this || family is null);
    return this.db.get(key, this, opts);
  }

  void putImpl(byte[] key, byte[] value, ColumnFamily family, WriteOptions opts = null) {
    assert(family == this || family is null);
    this.db.put(key, value, this, opts);
  }

  void removeImpl(byte[] key, ColumnFamily family, WriteOptions opts = null) {
    assert(family == this || family is null);
    this.db.remove(key, this, opts);
  }
}
