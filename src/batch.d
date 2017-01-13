module rocksdb.batch;

import std.string : toStringz;

extern (C) {
  struct rocksdb_writebatch_t {};

  rocksdb_writebatch_t* rocksdb_writebatch_create();
  rocksdb_writebatch_t* rocksdb_writebatch_create_from(const char*, size_t);
  void rocksdb_writebatch_destroy(rocksdb_writebatch_t*);
  void rocksdb_writebatch_clear(rocksdb_writebatch_t*);
  int rocksdb_writebatch_count(rocksdb_writebatch_t*);

  void rocksdb_writebatch_put(rocksdb_writebatch_t*, const char*, size_t, const char*, size_t);
  void rocksdb_writebatch_delete(rocksdb_writebatch_t*, const char*, size_t);
}

class WriteBatch {
  rocksdb_writebatch_t* batch;

  this() {
    this.batch = rocksdb_writebatch_create();
  }

  this(string frm) {
    this.batch = rocksdb_writebatch_create_from(toStringz(frm), frm.length);
  }

  ~this() {
    rocksdb_writebatch_destroy(this.batch);
  }

  void clear() {
    rocksdb_writebatch_clear(this.batch);
  }

  int count() {
    return rocksdb_writebatch_count(this.batch);
  }

  void put(string key, string value) {
    rocksdb_writebatch_put(this.batch, toStringz(key), key.length, toStringz(value), value.length);
  }
  
  void remove(string key) {
    rocksdb_writebatch_delete(this.batch, toStringz(key), key.length);
  }
}