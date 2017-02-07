module rocksdb.queryable;

import std.conv;

import rocksdb.options;

mixin template Getable() {
  public import std.conv : to;
  public import rocksdb.options : ReadOptions, WriteOptions;

  string get(string key, ReadOptions opts = null) {
    return (cast(char[])this.get(cast(byte[])key, opts)).to!string;
  }

  byte[] getBytes(string key, ReadOptions opts = null) {
    return this.get(cast(byte[])key, opts);
  }

  byte[] get(byte[] key, ReadOptions opts = null) {
    return this.getImpl(key, null, opts);
  }

}

mixin template Putable() {
  void put(string key, string value, WriteOptions opts = null) {
    this.put(cast(byte[])key, cast(byte[])value, opts);
  }

  void putBytes(string key, byte[] value, WriteOptions opts = null) {
    this.put(cast(byte[])key, value, opts);
  }

  void put(byte[] key, byte[] value, WriteOptions opts = null) {
    return this.putImpl(key, value, null, opts);
  }
}

mixin template Removeable() {
  void remove(string key, WriteOptions opts = null) {
    this.remove(cast(byte[])key, opts);
  }

  void remove(byte[] key, WriteOptions opts = null) {
    return this.removeImpl(key, null, opts);
  }
}
