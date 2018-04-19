package com.duprasville.guava.probably;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashCode;

import java.util.Random;

/**
 * Cuckoo Filter strategy that accepts an IndexingStrategy. This is compatible with any
 * IndexingStrategy that produces two invertible indices.
 *
 * @author Brian Dupras
 * @author Alex Beal
 */
public class IndexAgnosticCuckooStrategy extends AbstractCuckooStrategy {
  private static final int MAX_RELOCATION_ATTEMPTS = 500;
  private IndexingStrategy indexingStrategy;

  IndexAgnosticCuckooStrategy(int ordinal, IndexingStrategy indexingStrategy) {
    super(ordinal);

    this.indexingStrategy = indexingStrategy;
  }

  public long index(int hash, long m) {
    return indexingStrategy.index(hash, m);
  }

  public long altIndex(long index, int fingerprint, long m) {
    return indexingStrategy.altIndex(index, fingerprint, m);
  }

  private final Random kicker = new Random(1L);

  protected int pickEntryToKick(int numEntriesPerBucket) {
    return kicker.nextInt(numEntriesPerBucket);
  }

  protected long maxRelocationAttempts() {
    return MAX_RELOCATION_ATTEMPTS;
  }

  public <T> boolean add(T object, Funnel<? super T> funnel, CuckooTable table) {
    final long hash64 = hash(object, funnel).asLong();
    final int hash1 = hash1(hash64);
    final int hash2 = hash2(hash64);
    final int fingerprint = IndexingStrategyUtils.fingerprint(hash2, table.numBitsPerEntry);

    final long index = index(hash1, table.numBuckets);
    return putEntry(fingerprint, table, index) ||
            putEntry(fingerprint, table, altIndex(index, fingerprint, table.numBuckets));
  }

  public <T> boolean remove(T object, Funnel<? super T> funnel, CuckooTable table) {
    final long hash64 = hash(object, funnel).asLong();
    final int hash1 = hash1(hash64);
    final int hash2 = hash2(hash64);
    final int fingerprint = IndexingStrategyUtils.fingerprint(hash2, table.numBitsPerEntry);
    final long index1 = index(hash1, table.numBuckets);
    final long index2 = altIndex(index1, fingerprint, table.numBuckets);
    return table.swapAnyEntry(CuckooTable.EMPTY_ENTRY, fingerprint, index1)
            || table.swapAnyEntry(CuckooTable.EMPTY_ENTRY, fingerprint, index2);
  }

  public <T> boolean contains(T object, Funnel<? super T> funnel, CuckooTable table) {
    final long hash64 = hash(object, funnel).asLong();
    final int hash1 = hash1(hash64);
    final int hash2 = hash2(hash64);
    final int fingerprint = IndexingStrategyUtils.fingerprint(hash2, table.numBitsPerEntry);
    final long index1 = index(hash1, table.numBuckets);
    final long index2 = altIndex(index1, fingerprint, table.numBuckets);
    return table.hasEntry(fingerprint, index1) || table.hasEntry(fingerprint, index2);
  }

  private int hash1(long hash64) {
    return (int) hash64;
  }

  private int hash2(long hash64) {
    return (int) (hash64 >>> 32);
  }

  <T> HashCode hash(final T object, final Funnel<? super T> funnel) {
    return indexingStrategy.hashFunction().hashObject(object, funnel);
  }

  public int entriesPerBucket(double fpp) {
    return indexingStrategy.entriesPerBucket(fpp);
  }

  public long buckets(long capacity, int numEntriesPerBucket) {
    return indexingStrategy.buckets(capacity, numEntriesPerBucket);
  }

  public int bitsPerEntry(double fpp, int numEntriesPerBucket) {
    return indexingStrategy.bitsPerEntry(fpp, numEntriesPerBucket);
  }

}
