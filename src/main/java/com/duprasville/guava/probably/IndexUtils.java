package com.duprasville.guava.probably;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import static com.google.common.base.Preconditions.checkArgument;

public class IndexUtils {
    static final HashFunction hashFunction = Hashing.murmur3_128();

    /**
     * Returns an f-bit portion of the given hash. Iterating by f-bit segments from the least
     * significant side of the hash to the most significant, looks for a non-zero segment. If a
     * non-zero segment isn't found, 1 is returned to distinguish the fingerprint from a
     * non-entry.
     *
     * @param hash 32-bit hash value
     * @param f number of bits to consider from the hash
     * @return first non-zero f-bit value from hash as an int, or 1 if no non-zero value is found
     */
    public static int fingerprint(int hash, int f) {
      checkArgument(f > 0, "f must be greater than zero");
      checkArgument(f <= Integer.SIZE, "f must be less than " + Integer.SIZE);
      int mask = (0x80000000 >> (f - 1)) >>> (Integer.SIZE - f);

      for (int bit = 0; (bit + f) <= Integer.SIZE; bit += f) {
        int ret = (hash >> bit) & mask;
        if (0 != ret) {
          return ret;
        }
      }
      return 0x1;
    }

    static int hash(int i) {
      return hashFunction.hashInt(i).asInt();
    }

    /**
     * Returns the sum of index and offset, reduced by a mod-consistent amount if necessary to
     * protect from numeric overflow. This method is intended to support a subsequent mod operation
     * on the return value.
     *
     * @param index Assumed to be >= 0L.
     * @param offset Any value.
     * @param mod Value used to reduce the result,
     * @return sum of index and offset, reduced by a mod-consistent amount if necessary to protect
     *         from numeric overflow.
     */
    static long protectedSum(long index, long offset, long mod) {
      return canSum(index, offset) ? index + offset : protectedSum(index - mod, offset, mod);
    }

    static boolean canSum(long a, long b) {
      return (a ^ b) < 0 | (a ^ (a + b)) >= 0;
    }
}
