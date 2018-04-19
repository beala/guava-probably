package com.duprasville.guava.probably;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.math.LongMath;

import java.math.RoundingMode;

import static com.duprasville.guava.probably.CuckooFilter.optimalBitsPerEntry;
import static com.duprasville.guava.probably.CuckooFilter.optimalEntriesPerBucket;
import static com.duprasville.guava.probably.CuckooFilter.optimalNumberOfBuckets;
import static com.google.common.math.LongMath.mod;


public class OriginalIndexingStrategy implements IndexingStrategy {

  private HashFunction hashFunction = Hashing.murmur3_128();

  public HashFunction hashFunction() {
    return hashFunction;
  }

  public long index(int hash, long m) {
    return mod(hash, m);
  }

  public long altIndex(long index, int fingerprint, long m) {
    return mod(index ^ hash(fingerprint), m);
  }

  public int entriesPerBucket(double fpp) {
    return optimalEntriesPerBucket(fpp);
  }

  public long buckets(long capacity, int numEntriesPerBucket) {
    return nextPowerOfTwo(optimalNumberOfBuckets(capacity, numEntriesPerBucket));
  }

  public int bitsPerEntry(double fpp, int numEntriesPerBucket) {
    return optimalBitsPerEntry(fpp, numEntriesPerBucket);
  }

  private static long nextPowerOfTwo(long n) {
    return LongMath.pow(
            2,
            LongMath.log2(n, RoundingMode.CEILING)
    );
  }

  private int hash(int i) {
    return hashFunction().hashInt(i).asInt();
  }

}
