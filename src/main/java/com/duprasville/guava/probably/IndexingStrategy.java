package com.duprasville.guava.probably;

import com.google.common.hash.HashFunction;

import java.io.Serializable;

public interface IndexingStrategy extends Serializable {
  HashFunction hashFunction();

  long index(int hash, long m);

  long altIndex(long index, int fingerprint, long m);

  int entriesPerBucket(double fpp);

  long buckets(long capacity, int numEntriesPerBucket);

  int bitsPerEntry(double fpp, int numEntriesPerBucket);
}
