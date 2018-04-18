/*
 * Copyright (C) 2015 Brian Dupras
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */


package com.duprasville.guava.probably;

import com.google.common.math.LongMath;
import org.junit.Test;

import java.math.RoundingMode;
import java.util.Random;

import static com.duprasville.guava.probably.CuckooStrategies.MURMUR128_BEALDUPRAS_32;
import static com.duprasville.guava.probably.CuckooStrategies.ORIGINAL;
import static com.duprasville.guava.probably.CuckooStrategies.values;
import static com.duprasville.guava.probably.CuckooTable.readBits;
import static com.duprasville.guava.probably.CuckooTable.writeBits;
import static com.google.common.truth.Truth.assertThat;
import static junit.framework.Assert.assertEquals;

/**
 * CuckooFilterStrategies tests. These are tests of internal, though somewhat complex,
 * implementation details of cuckoo filters.
 *
 * @author Brian Dupras
 */
public abstract class AbstractCuckooStrategiesTest {

  abstract public AbstractCuckooStrategy strategy();

  @Test
  public void fingerprintBoundaries() throws Exception {
    assertThat(IndexUtils.fingerprint(0x80000000, 1)).isEqualTo(0x01);
    assertThat(IndexUtils.fingerprint(0xC0000000, 2)).isEqualTo(0x03);
    assertThat(IndexUtils.fingerprint(0xE0000000, 3)).isEqualTo(0x04);
    assertThat(IndexUtils.fingerprint(0xE0000000, 8)).isEqualTo(0xE0);
    assertThat(IndexUtils.fingerprint(0xE0000000, 16)).isEqualTo(0xE000);
    assertThat(IndexUtils.fingerprint(0x80000000, Integer.SIZE)).isEqualTo(0x80000000);
    for (int f = 1; f < Integer.SIZE; f++) {
      assertThat(IndexUtils.fingerprint(0x00, f)).isNotEqualTo(0x00);
    }
  }

  @Test
  public void indexIsModuloM() throws Exception {
    final int min = Integer.MIN_VALUE;
    final int max = Integer.MAX_VALUE;
    final int incr = 100000;
    final long m = 0x1DEAL;

    for (int hash = min; hash != next(hash, incr, max); hash = next(hash, incr, max)) {
      final long index = strategy().index(hash, m);
      assertThat(index).isLessThan(m);
      assertThat(index).isGreaterThan(-1L);
    }
  }

  @Test
  public abstract void altIndexIsReversible() throws Exception;

  /**
   * This test will fail whenever someone updates/reorders the BloomFilterStrategies constants. Only
   * appending a new constant is allowed.
   */
  @Test
  public void cuckooFilterStrategies() {
    assertThat(values()).hasLength(2);
    assertEquals(MURMUR128_BEALDUPRAS_32, values()[0]);
    assertEquals(ORIGINAL, values()[1]);
  }

  @Test
  public void writeBits_() throws Exception {
    long[] data;

    data = new long[]{0xfafafafafafafafal, 0xfafafafafafafafal};
    assertEquals(0xfafa, writeBits(0xABCD, data, 0, 16));
    assertEquals(0xfafafafafafaABCDl, data[0]);
    assertEquals(0xfafafafafafafafal, data[1]);

    data = new long[]{0xfafafafafafafafal, 0xfafafafafafafafal};
    assertEquals(0xfafa, writeBits(0xABCD, data, 32, 16));
    assertEquals(0xfafaABCDfafafafal, data[0]);
    assertEquals(0xfafafafafafafafal, data[1]);

    data = new long[]{0xfafafafafafafafal, 0xfafafafafafafafal};
    assertEquals(0xfafa, writeBits(0xABCD, data, 48, 16));
    assertEquals(0xABCDfafafafafafal, data[0]);
    assertEquals(0xfafafafafafafafal, data[1]);

    data = new long[]{0xfafafafafafafafal, 0xfafafafafafafafal};
    assertEquals(0x7D7D, writeBits(0xABCD, data, 49, 16));
    assertEquals(0x579Afafafafafafal, data[0]);
    assertEquals(0xfafafafafafafafBl, data[1]);

    data = new long[]{0xfafafafafafafafal, 0xfafafafafafafafal};
    assertEquals(0xfafa, writeBits(0xABCD, data, 56, 16));
    assertEquals(0xCDfafafafafafafal, data[0]);
    assertEquals(0xfafafafafafafaABl, data[1]);

    data = new long[]{0xfafafafafafafafal, 0xfafafafafafafafal};
    assertEquals(0xfafa, writeBits(0xABCD, data, 64, 16));
    assertEquals(0xfafafafafafafafal, data[0]);
    assertEquals(0xfafafafafafaABCDl, data[1]);

    data = new long[]{0xfafafafafafafafal, 0xfafafafafafafafal};
    assertEquals(0xfafa, writeBits(0xABCD, data, 112, 16));
    assertEquals(0xfafafafafafafafal, data[0]);
    assertEquals(0xABCDfafafafafafal, data[1]);
  }

  @Test
  public void readBits_() throws Exception {
    assertEquals(0xABCD, readBits(new long[]{0x000000000000ABCDL, 0x00000000000000FFL}, 0, 16));
    assertEquals(0xABCD, readBits(new long[]{0x0000ABCD00000000L, 0x00000000000000FFL}, 32, 16));
    assertEquals(0xABCD, readBits(new long[]{0xABCD000000000000L, 0x00000000000000FFL}, 48, 16));
    assertEquals(0xABCD, readBits(new long[]{0xABCD000000000000L << 1, 0x00000000000FFL}, 49, 16));
    assertEquals(0xABCD, readBits(new long[]{0xCD00000000000000L, 0x0000000000000FABL}, 56, 16));
    assertEquals(0xABCD, readBits(new long[]{0xFF00000000000000L, 0x000000000000ABCDL}, 64, 16));

    assertEquals(0x01CD, readBits(new long[]{0x000000000000ABCDL, 0x00000000000000FFL}, 0, 9));
    assertEquals(0x01CD, readBits(new long[]{0x0000ABCD00000000L, 0x00000000000000FFL}, 32, 9));
    assertEquals(0x01CD, readBits(new long[]{0xABCD000000000000L, 0x00000000000000FFL}, 48, 9));
    assertEquals(0x01CD, readBits(new long[]{0xABCD000000000000L << 1, 0x00000000000FFL}, 49, 9));
    assertEquals(0x01CD, readBits(new long[]{0xCD00000000000000L, 0x0000000000000FABL}, 56, 9));
    assertEquals(0x01CD, readBits(new long[]{0xFF00000000000000L, 0x000000000000ABCDL}, 64, 9));
  }


  // Test utilities

  int next(int start, int incr, int max) {
    int ret = start + max / incr;
    return ((ret < start) || (ret > max)) ? max : ret;
  }

  long next(long start, long incr, long max) {
    long ret = start + max / incr;
    return ((ret < start) || (ret > max)) ? max : ret;
  }
}
