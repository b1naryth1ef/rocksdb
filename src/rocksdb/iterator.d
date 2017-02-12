module rocksdb.iterator;

import std.conv : to;
import std.string : fromStringz, toStringz;

import rocksdb.slice : Slice;
import rocksdb.options : ReadOptions, rocksdb_readoptions_t;
import rocksdb.database : Database, rocksdb_t;
import rocksdb.columnfamily : ColumnFamily, rocksdb_column_family_handle_t;

extern (C) {
  struct rocksdb_iterator_t {};

  rocksdb_iterator_t* rocksdb_create_iterator(rocksdb_t*, rocksdb_readoptions_t*);
  rocksdb_iterator_t* rocksdb_create_iterator_cf(rocksdb_t*, rocksdb_readoptions_t*, rocksdb_column_family_handle_t*);

  void rocksdb_iter_destroy(rocksdb_iterator_t*);
  ubyte rocksdb_iter_valid(const rocksdb_iterator_t*);
  void rocksdb_iter_seek_to_first(rocksdb_iterator_t*);
  void rocksdb_iter_seek_to_last(rocksdb_iterator_t*);
  void rocksdb_iter_seek(rocksdb_iterator_t*, const char*, size_t);
  void rocksdb_iter_seek_for_prev(rocksdb_iterator_t*, const char*, size_t);
  void rocksdb_iter_next(rocksdb_iterator_t*);
  void rocksdb_iter_prev(rocksdb_iterator_t*);
  immutable(char*) rocksdb_iter_key(const rocksdb_iterator_t*, size_t*);
  immutable(char*) rocksdb_iter_value(const rocksdb_iterator_t*, size_t*);
  void rocksdb_iter_get_error(const rocksdb_iterator_t*, char**);
}


class Iterator {
  rocksdb_iterator_t* iter;

  this(Database db, ReadOptions opts) {
    this.iter = rocksdb_create_iterator(db.db, opts.opts);
    this.seekToFirst();
  }

  this(Database db, ColumnFamily family, ReadOptions opts) {
    this.iter = rocksdb_create_iterator_cf(db.db, opts.opts, family.cf);
    this.seekToFirst();
  }

  ~this() {
    rocksdb_iter_destroy(this.iter);
  }

  void seekToFirst() {
    rocksdb_iter_seek_to_first(this.iter);
  }

  void seekToLast() {
    rocksdb_iter_seek_to_last(this.iter);
  }

  void seek(string key) {
    this.seek(cast(ubyte[])key);
  }

  void seek(in ubyte[] key) {
    rocksdb_iter_seek(this.iter, cast(char*)key.ptr, key.length);
  }

  void seekPrev(string key) {
    this.seekPrev(cast(ubyte[])key);
  }

  void seekPrev(in ubyte[] key) {
    rocksdb_iter_seek_for_prev(this.iter, cast(char*)key.ptr, key.length);
  }

  void next() {
    rocksdb_iter_next(this.iter);
  }

  void prev() {
    rocksdb_iter_prev(this.iter);
  }

  bool valid() {
    return cast(bool)rocksdb_iter_valid(this.iter);
  }

  ubyte[] key() {
    size_t size;
    immutable char* ckey = rocksdb_iter_key(this.iter, &size);
    return cast(ubyte[])ckey[0..size];
  }

  Slice keySlice() {
    size_t size;
    immutable char* ckey = rocksdb_iter_key(this.iter, &size);
    return Slice(size, ckey);
  }

  ubyte[] value() {
    size_t size;
    immutable char* cvalue = rocksdb_iter_value(this.iter, &size);
    return cast(ubyte[])cvalue[0..size];
  }

  Slice valueSlice() {
    size_t size;
    immutable char* cvalue = rocksdb_iter_value(this.iter, &size);
    return Slice(size, cvalue);
  }

  /*
  int opApply(scope int delegate(ref string, ref string) dg) {
    int result = 0;

    foreach (key, value; this) {
      result = dg(cast(string)key, cast(string)value);
      if (result) break;
    }

    return result;
  }
  */

  int opApply(scope int delegate(ubyte[], ubyte[]) dg) {
    int result = 0;

    while (this.valid()) {
      result = dg(this.key(), this.value());
      if (result) break;
      this.next();
    }

    return result;
  }

  void close() {
    destroy(this);
  }
}
