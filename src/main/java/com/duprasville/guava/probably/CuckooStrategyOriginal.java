package com.duprasville.guava.probably;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashCode;
import com.google.common.math.LongMath;

import java.math.RoundingMode;
import java.util.Random;

import static com.duprasville.guava.probably.CuckooFilter.optimalBitsPerEntry;
import static com.duprasville.guava.probably.CuckooFilter.optimalEntriesPerBucket;
import static com.duprasville.guava.probably.CuckooFilter.optimalNumberOfBuckets;
import static com.google.common.math.LongMath.mod;

public class CuckooStrategyOriginal extends AbstractCuckooStrategy implements CuckooStrategy {

    private static final int MAX_RELOCATION_ATTEMPTS = 500;

    CuckooStrategyOriginal(int ordinal) {
        super(ordinal);
    }

    public long index(int hash, long m) {
        return mod(hash, m);
    }

    public long altIndex(long index, int fingerprint, long m) {
        return mod(index ^ IndexUtils.hash(fingerprint), m);
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
        final int fingerprint = IndexUtils.fingerprint(hash2, table.numBitsPerEntry);

        final long index = index(hash1, table.numBuckets);
        return putEntry(fingerprint, table, index) ||
                putEntry(fingerprint, table, altIndex(index, fingerprint, table.numBuckets));
    }

    public <T> boolean remove(T object, Funnel<? super T> funnel, CuckooTable table) {
        final long hash64 = hash(object, funnel).asLong();
        final int hash1 = hash1(hash64);
        final int hash2 = hash2(hash64);
        final int fingerprint = IndexUtils.fingerprint(hash2, table.numBitsPerEntry);
        final long index1 = index(hash1, table.numBuckets);
        final long index2 = altIndex(index1, fingerprint, table.numBuckets);
        return table.swapAnyEntry(CuckooTable.EMPTY_ENTRY, fingerprint, index1)
                || table.swapAnyEntry(CuckooTable.EMPTY_ENTRY, fingerprint, index2);
    }

    public <T> boolean contains(T object, Funnel<? super T> funnel, CuckooTable table) {
        final long hash64 = hash(object, funnel).asLong();
        final int hash1 = hash1(hash64);
        final int hash2 = hash2(hash64);
        final int fingerprint = IndexUtils.fingerprint(hash2, table.numBitsPerEntry);
        final long index1 = index(hash1, table.numBuckets);
        final long index2 = altIndex(index1, fingerprint, table.numBuckets);
        return table.hasEntry(fingerprint, index1) || table.hasEntry(fingerprint, index2);
    }

    int hash1(long hash64) {
        return (int) hash64;
    }

    int hash2(long hash64) {
        return (int) (hash64 >>> 32);
    }

    <T> HashCode hash(final T object, final Funnel<? super T> funnel) {
        return IndexUtils.hashFunction.hashObject(object, funnel);
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
}
