module rocksdb.queryable;

import std.conv;

import rocksdb.options,
       rocksdb.columnfamily;

/**
TODO:
  these need refactoring to remove the weird fuckery of options
*/

mixin template Getable() {
  public import std.conv : to;
  public import rocksdb.options : ReadOptions, WriteOptions;

  string get(string key, ReadOptions opts = null) {
    return this.get(key, null, opts);
  }

  string get(string key, ColumnFamily family, ReadOptions opts = null) {
    return (cast(char[])this.get(cast(byte[])key, family, opts)).to!string;
  }

  byte[] getBytes(string key, ReadOptions opts = null) {
    return this.getBytes(key, null, opts);
  }

  byte[] getBytes(string key, ColumnFamily family, ReadOptions opts = null) {
    return this.get(cast(byte[])key, family, opts);
  }

  byte[] get(byte[] key, ReadOptions opts = null) {
    return this.get(key, null, opts);
  }

  byte[] get(byte[] key, ColumnFamily family, ReadOptions opts = null) {
    return this.getImpl(key, family, opts);
  }

}

mixin template Putable() {
  void put(string key, string value, WriteOptions opts = null) {
    this.put(key, value, null, opts);
  }

  void put(string key, string value, ColumnFamily family, WriteOptions opts = null) {
    this.put(cast(byte[])key, cast(byte[])value, opts);
  }

  void putBytes(string key, byte[] value, WriteOptions opts = null) {
    this.putBytes(key, value, null, opts);
  }

  void putBytes(string key, byte[] value, ColumnFamily family, WriteOptions opts = null) {
    this.put(cast(byte[])key, value, family, opts);
  }

  void put(byte[] key, byte[] value, WriteOptions opts = null) {
    this.put(key, value, null, opts);
  }

  void put(byte[] key, byte[] value, ColumnFamily family, WriteOptions opts = null) {
    return this.putImpl(key, value, family, opts);
  }
}

mixin template Removeable() {
  void remove(string key, WriteOptions opts = null) {
    this.remove(key, null, opts);
  }

  void remove(string key, ColumnFamily family, WriteOptions opts = null) {
    this.remove(cast(byte[])key, family, opts);
  }

  void remove(byte[] key, WriteOptions opts = null) {
    this.remove(key, null, opts);
  }

  void remove(byte[] key, ColumnFamily family, WriteOptions opts = null) {
    return this.removeImpl(key, family, opts);
  }
}
