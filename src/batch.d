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

  void put(byte[] key, byte[] value) {
    rocksdb_writebatch_put(this.batch,
      cast(char*)key.ptr, key.length,
      cast(char*)value.ptr, value.length);
  }

  void put(string key, string value) {
    this.put(cast(byte[])key, cast(byte[])value);
  }
  
  void remove(byte[] key) {
    rocksdb_writebatch_delete(this.batch, cast(char*)key.ptr, key.length);
  }

  void remove(string key) {
    this.remove(cast(byte[])key);
  }

}