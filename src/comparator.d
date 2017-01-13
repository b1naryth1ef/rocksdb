module rocksdb.comparator;

extern (C) {
  struct rocksdb_comparator_t {};

  rocksdb_comparator_t* rocksdb_comparator_create(
    void* state,
    void function(void*),
    int function(void*, const char* a, size_t alen, const char* b, size_t blen),
    immutable(char*) function(void*));

  void rocksdb_comparator_destroy(rocksdb_comparator_t*);

  alias CmpDestroyF = void function(void*);
  alias CmpCompareF = static int function(void* arg, const char* a, size_t alen, const char* b, size_t blen);
  alias CmpNameF = static immutable(char*) function(void *arg);
}


class Comparator {
  rocksdb_comparator_t* cmp;

  this(CmpDestroyF des, CmpCompareF cmp, CmpNameF name) {
    this.cmp = rocksdb_comparator_create(null, des, cmp, name);
  }

  ~this() {
    rocksdb_comparator_destroy(this.cmp);
  }
}

unittest {
  import std.algorithm.comparison : max;
  import core.stdc.string : cmemcmp = memcmp;
  import std.string : toStringz;

  extern (C) void destroyComparator(void *arg) {

  }

  extern (C) int compareComparator(void *arg, const char* a, size_t aSize, const char* b, size_t bSize) {
    size_t n = max(aSize, bSize);
    int r = cmemcmp(a, b, n);
    if (r == 0) {
      r = (aSize < bSize) ? -1 : 1;
    }
    return r;
  }

  extern (C) static immutable(char)* nameComparator(void *arg) {
    return toStringz("test");
  }

  auto cmp = new Comparator(&destroyComparator, &compareComparator, &nameComparator);
  destroy(cmp);
}