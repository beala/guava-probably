package com.duprasville.guava.probably;

import static com.google.common.base.Preconditions.checkArgument;

public class IndexingStrategyUtils {
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

}
