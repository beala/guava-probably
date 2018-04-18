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

import com.google.common.collect.ImmutableSet;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.PrimitiveSink;
import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import com.google.common.testing.SerializableTester;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.math.RoundingMode;
import java.util.Random;

import javax.annotation.Nullable;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.truth.Truth.assertThat;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotSame;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

/**
 * CuckooFilter tests of Object method overrides, static methods, and serialization. Modified from
 * Guava tests for BloomFilter.
 *
 * @author Brian Dupras
 * @author The Guava Authors (original BloomFilter tests)
 */
public abstract class AbstractCuckooFilterTest {
  
  abstract AbstractCuckooStrategy strategy();
  
  @SuppressWarnings("ResultOfMethodCallIgnored")
  void createAndCheckCuckooFilterWithKnownFalsePositives(
          ImmutableSet<Integer> knownFalsePositives,
          int expectedNumFp
  ) {
    int numInsertions = 1000000;
    CuckooFilter<String> cf = CuckooFilter.create(
        Funnels.unencodedCharsFunnel(), numInsertions, 0.03,
        strategy());

    // Insert "numInsertions" even numbers into the CF.
    for (int i = 0; i < numInsertions * 2; i += 2) {
      cf.add(Integer.toString(i));
    }

    // Assert that the CF "might" have all of the even numbers.
    for (int i = 0; i < numInsertions * 2; i += 2) {
      assertTrue(cf.contains(Integer.toString(i)));
    }

    // Now we check for known false positives using a set of known false positives.
    for (int i = 1; i < 900; i += 2) {
      if (!knownFalsePositives.contains(i)) {
        assertFalse("CF should not contain " + i, cf.contains(Integer.toString(i)));
      }
    }

    // Check that there are the right number of false positives for this CF.
    int actualNumFpp = 0;
    for (int i = 1; i < numInsertions * 2; i += 2) {
      if (cf.contains(Integer.toString(i))) {
        actualNumFpp++;
      }
    }
    assertEquals(expectedNumFp, actualNumFpp);
    // The normal order of (expected, actual) is reversed here on purpose.
    assertEquals((double) expectedNumFp / numInsertions, cf.currentFpp(), 0.00035);
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  void createAndCheckCuckooFilterWithKnownUtf8FalsePositives(
          ImmutableSet<Integer> knownFalsePositives,
          int expectedNumFp
  ) {
    int numInsertions = 1000000;
    CuckooFilter<String> cf = CuckooFilter.create(
        Funnels.stringFunnel(UTF_8), numInsertions, 0.03,
        strategy());

    // Insert "numInsertions" even numbers into the CF.
    for (int i = 0; i < numInsertions * 2; i += 2) {
      cf.add(Integer.toString(i));
    }

    // Assert that the CF "might" have all of the even numbers.
    for (int i = 0; i < numInsertions * 2; i += 2) {
      assertTrue(cf.contains(Integer.toString(i)));
    }

    // Now we check for known false positives using a set of known false positives.
    // (These are all of the false positives under 900.)
    for (int i = 1; i < 900; i += 2) {
      if (!knownFalsePositives.contains(i)) {
        assertFalse("CF should not contain " + i, cf.contains(Integer.toString(i)));
      }
    }

    // Check that there are the correct number of false positives for this CF.
    int actualNumFpp = 0;
    for (int i = 1; i < numInsertions * 2; i += 2) {
      if (cf.contains(Integer.toString(i))) {
        actualNumFpp++;
      }
    }
    assertEquals(expectedNumFp, actualNumFpp);
    // The normal order of (expected, actual) is reversed here on purpose.
    assertEquals((double) expectedNumFp / numInsertions, cf.currentFpp(), 0.0004);
  }

  /**
   * Sanity checking with many combinations of false positive rates and expected insertions
   */
  @Test
  public void basic() {
    for (double fpr = 0.0000001; fpr < 0.1; fpr *= 10) {
      for (int capacity = 1; capacity <= 10000; capacity *= 10) {
        final CuckooFilter<Object> cf = CuckooFilter.create(BAD_FUNNEL,
            capacity, fpr, strategy());

        assertFalse(cf.contains(new Object()));
        for (int insertions = 0; insertions < capacity; insertions++) {
          Object o = new Object();
          if (cf.add(o)) {
            assertTrue("mightContain should return true when queried with an object previously " +
                "added to the filter", cf.contains(o));
          }
        }
      }
    }
  }

  @SuppressWarnings({"CheckReturnValue", "ResultOfMethodCallIgnored"})
  @Test
  public void preconditions() {
    try {
      CuckooFilter.create(Funnels.unencodedCharsFunnel(), -1, strategy());
      fail();
    } catch (IllegalArgumentException expected) {
    }
    try {
      CuckooFilter.create(Funnels.unencodedCharsFunnel(), -1, 0.03, strategy());
      fail();
    } catch (IllegalArgumentException expected) {
    }
    try {
      CuckooFilter.create(Funnels.unencodedCharsFunnel(), 1, 0.0, strategy());
      fail();
    } catch (IllegalArgumentException expected) {
    }
    try {
      CuckooFilter.create(Funnels.unencodedCharsFunnel(), 1, 1.0, strategy());
      fail();
    } catch (IllegalArgumentException expected) {
    }
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  public void failureWhenMoreThan64BitFingerprintsAreNeeded() {
    try {
      int n = 1000;
      double p = 0.00000000000000000000000000000000000000000000000000000000000000000000000000000001;
      CuckooFilter.create(Funnels.unencodedCharsFunnel(), n, p, strategy());
      fail();
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void nullPointers() {
    NullPointerTester tester = new NullPointerTester();
    tester.testAllPublicInstanceMethods(CuckooFilter.create(Funnels.unencodedCharsFunnel(), 100, strategy()));
    tester.testAllPublicStaticMethods(CuckooFilter.class);
  }

  /**
   * Tests that we always get a non-negative optimal size.
   */
  @SuppressWarnings({"CheckReturnValue", "ResultOfMethodCallIgnored"})
  @Test
  public void optimalSize() {
    for (int n = 1; n < 1000; n++) {
      for (double fpp = CuckooFilter.MIN_FPP; fpp <= CuckooFilter.MAX_FPP; fpp += 0.001) {
        assertTrue(CuckooFilter.optimalEntriesPerBucket(fpp) >= 2);
        assertTrue(
            CuckooFilter.optimalNumberOfBuckets(n, CuckooFilter.optimalEntriesPerBucket(fpp)) >= 2);
        assertTrue(
            CuckooFilter.optimalBitsPerEntry(fpp, CuckooFilter.optimalEntriesPerBucket(fpp)) >= 2);
      }
    }

    // some random values
    Random random = new Random(0);
    for (int repeats = 0; repeats < 10000; repeats++) {
      final int n = random.nextInt(1 << 16);
      final double fpp = random.nextDouble();

      assertTrue(CuckooFilter.optimalEntriesPerBucket(fpp) >= 2);
      assertTrue(
          CuckooFilter.optimalNumberOfBuckets(n, CuckooFilter.optimalEntriesPerBucket(fpp)) >= 2);
      assertTrue(
          CuckooFilter.optimalBitsPerEntry(fpp, CuckooFilter.optimalEntriesPerBucket(fpp)) >= 2);
    }

    assertEquals(8, CuckooFilter.optimalEntriesPerBucket(CuckooFilter.MIN_FPP));
    assertEquals(2, CuckooFilter.optimalEntriesPerBucket(CuckooFilter.MAX_FPP));
    assertEquals(273913732, CuckooFilter.optimalNumberOfBuckets(Integer.MAX_VALUE,
        CuckooFilter.optimalEntriesPerBucket(CuckooFilter.MIN_FPP)));
    assertEquals(Long.SIZE, CuckooFilter.optimalBitsPerEntry(CuckooFilter.MIN_FPP,
        CuckooFilter.optimalEntriesPerBucket(CuckooFilter.MIN_FPP)));

    try {
      CuckooFilter.create(BAD_FUNNEL, Integer.MAX_VALUE, Double.MIN_VALUE, strategy());
      fail("we can't represent a CF with such an FPP lower than " + CuckooFilter.MIN_FPP + "!");
    } catch (IllegalArgumentException expected) {
      assertThat(expected).hasMessage("Cannot create CuckooFilter with FPP[4.9E-324] < " +
          "CuckooFilter.MIN_FPP[8.673617379884035E-19]");
    }
  }

  @Test
  public void copy() {
    CuckooFilter<String> original = CuckooFilter.create(Funnels.unencodedCharsFunnel(), 100, strategy());
    CuckooFilter<String> copy = original.copy();
    assertNotSame(original, copy);
    assertEquals(original, copy);
  }

  @Test
  public void bitSize() {
    double fpp = 0.03;
    for (int i = 1; i < 10000; i++) {
      CuckooFilter<CharSequence> filter = CuckooFilter.create(Funnels.unencodedCharsFunnel(), i, fpp, strategy());
      long numBits = filter.calculateDataLength(i, fpp) * Long.SIZE;
      int arraySize = Ints.checkedCast(LongMath.divide(numBits, Long.SIZE, RoundingMode.CEILING));
      assertEquals(
          arraySize * Long.SIZE,
          filter.bitSize());
    }
  }

  @Test
  public void equals_empty() {
    new EqualsTester()
        .addEqualityGroup(CuckooFilter.create(Funnels.byteArrayFunnel(), 100, 0.01, strategy()))
        .addEqualityGroup(CuckooFilter.create(Funnels.byteArrayFunnel(), 100, 0.02, strategy()))
        .addEqualityGroup(CuckooFilter.create(Funnels.byteArrayFunnel(), 200, 0.01, strategy()))
        .addEqualityGroup(CuckooFilter.create(Funnels.byteArrayFunnel(), 200, 0.02, strategy()))
        .addEqualityGroup(CuckooFilter.create(Funnels.unencodedCharsFunnel(), 100, 0.01, strategy()))
        .addEqualityGroup(CuckooFilter.create(Funnels.unencodedCharsFunnel(), 100, 0.02, strategy()))
        .addEqualityGroup(CuckooFilter.create(Funnels.unencodedCharsFunnel(), 200, 0.01, strategy()))
        .addEqualityGroup(CuckooFilter.create(Funnels.unencodedCharsFunnel(), 200, 0.02, strategy()))
        .testEquals();
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  public void equals() {
    CuckooFilter<String> cf1 = CuckooFilter.create(Funnels.unencodedCharsFunnel(), 100, strategy());
    cf1.add("1");
    cf1.add("2");

    CuckooFilter<String> cf2 = CuckooFilter.create(Funnels.unencodedCharsFunnel(), 100, strategy());
    cf2.add("1");
    cf2.add("2");

    new EqualsTester()
        .addEqualityGroup(cf1, cf2)
        .testEquals();

    cf2.add("3");

    new EqualsTester()
        .addEqualityGroup(cf1)
        .addEqualityGroup(cf2)
        .testEquals();

    cf2.remove("3");

    new EqualsTester()
        .addEqualityGroup(cf1, cf2)
        .testEquals();

  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  public void equals2() {
    // numInsertions param undersized purposely to force underlying storage saturation
    CuckooFilter<String> cf1 = CuckooFilter.create(Funnels.unencodedCharsFunnel(), 2, strategy());
    cf1.add("1");
    cf1.add("2");
    cf1.add("3");
    cf1.add("4");

    CuckooFilter<String> cf2 = CuckooFilter.create(Funnels.unencodedCharsFunnel(), 2, strategy());
    cf2.add("4");
    cf2.add("3");
    cf2.add("2");
    cf2.add("1");

    assertTrue("equals should be true when tables are equivalent but ordered differently",
        cf1.equals(cf2));

    new EqualsTester()
        .addEqualityGroup(cf1, cf2)
        .testEquals();
  }

  @Test
  public void equalsWithCustomFunnel() {
    CuckooFilter<Long> cf1 = CuckooFilter.create(new CustomFunnel(), 100, strategy());
    CuckooFilter<Long> cf2 = CuckooFilter.create(new CustomFunnel(), 100, strategy());
    assertEquals(cf1, cf2);
  }

  @Test
  public void serializationWithCustomFunnel() {
    SerializableTester.reserializeAndAssert(CuckooFilter.create(new CustomFunnel(), 100, strategy()));
  }

  private static final class CustomFunnel implements Funnel<Long> {
    public void funnel(Long value, PrimitiveSink into) {
      into.putLong(value);
    }

    @Override
    public boolean equals(@Nullable Object object) {
      return (object instanceof CustomFunnel);
    }

    @Override
    public int hashCode() {
      return 42;
    }
  }

  @Test
  public void addReturnValue() {
    for (int i = 0; i < 10; i++) {
      CuckooFilter<String> cf = CuckooFilter.create(Funnels.unencodedCharsFunnel(), 100, strategy());
      for (int j = 0; j < 10; j++) {
        String value = new Object().toString();
        boolean mightContain = cf.contains(value);
        boolean put = cf.add(value);
        assertTrue(mightContain != put);
        boolean delete = cf.remove(value);
        assertTrue(put == delete);
      }
    }
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  public void addAll() {
    int element1 = 1;
    int element2 = 2;

    CuckooFilter<Integer> cf1 = CuckooFilter.create(Funnels.integerFunnel(), 100, strategy());
    cf1.add(element1);
    assertTrue(cf1.contains(element1));
    assertFalse(cf1.contains(element2));

    CuckooFilter<Integer> cf2 = CuckooFilter.create(Funnels.integerFunnel(), 100, strategy());
    cf2.add(element2);
    assertFalse(cf2.contains(element1));
    assertTrue(cf2.contains(element2));

    assertTrue(cf1.isCompatible(cf2));
    cf1.addAll(cf2);
    assertTrue(cf1.contains(element1));
    assertTrue(cf1.contains(element2));
    assertFalse(cf2.contains(element1));
    assertTrue(cf2.contains(element2));
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  public void addAllFails() {
    int element = 1;

    CuckooFilter<Integer> cf1 = CuckooFilter.create(Funnels.integerFunnel(), 100, strategy());
    // purposely fill buckets that contain entries for element
    while (cf1.add(element)) {
      assertTrue(cf1.contains(element));
    }

    CuckooFilter<Integer> cf2 = CuckooFilter.create(Funnels.integerFunnel(), 100, strategy());
    cf2.add(element);
    assertTrue(cf2.contains(element));

    assertTrue(cf1.isCompatible(cf2));

    assertFalse("putAll should return false when buckets at index & altIndex are already full",
        cf1.addAll(cf2));
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  public void addAllDifferentSizes() {
    CuckooFilter<Integer> cf1 = CuckooFilter.create(Funnels.integerFunnel(), 1, strategy());
    CuckooFilter<Integer> cf2 = CuckooFilter.create(Funnels.integerFunnel(), 10, strategy());

    try {
      assertFalse(cf1.isCompatible(cf2));
      cf1.addAll(cf2);
      fail();
    } catch (IllegalArgumentException expected) {
    }

    try {
      assertFalse(cf2.isCompatible(cf1));
      cf2.addAll(cf1);
      fail();
    } catch (IllegalArgumentException expected) {
    }
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  public void addAllWithSelf() {
    CuckooFilter<Integer> cf1 = CuckooFilter.create(Funnels.integerFunnel(), 1, strategy());
    try {
      assertFalse(cf1.isCompatible(cf1));
      cf1.addAll(cf1);
      fail();
    } catch (IllegalArgumentException expected) {
    }
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  public void javaSerialization() {
    CuckooFilter<byte[]> cf = CuckooFilter.create(Funnels.byteArrayFunnel(), 100, strategy());
    for (int i = 0; i < 10; i++) {
      cf.add(Ints.toByteArray(i));
    }

    CuckooFilter<byte[]> copy = SerializableTester.reserialize(cf);
    for (int i = 0; i < 10; i++) {
      assertTrue(copy.contains(Ints.toByteArray(i)));
    }
    assertEquals(cf.currentFpp(), copy.currentFpp());

    SerializableTester.reserializeAndAssert(cf);
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  public void customSerialization() throws Exception {
    Funnel<byte[]> funnel = Funnels.byteArrayFunnel();
    CuckooFilter<byte[]> cf = CuckooFilter.create(funnel, 100, strategy());
    for (int i = 0; i < 100; i++) {
      cf.add(Ints.toByteArray(i));
    }

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    cf.writeTo(out);

    assertEquals(cf, CuckooFilter.readFrom(new ByteArrayInputStream(out.toByteArray()), funnel));
  }

  static final Funnel<Object> BAD_FUNNEL = new Funnel<Object>() {
    public void funnel(Object object, PrimitiveSink bytePrimitiveSink) {
      bytePrimitiveSink.putInt(object.hashCode());
    }
  };

  @Test
  public void ensureGeneric() {
    class SuperClass {
    }
    class SubClass extends SuperClass {
    }

    CuckooFilter<SuperClass> filter = CuckooFilter.create(
        new Funnel<SuperClass>() {
          public void funnel(SuperClass from, PrimitiveSink into) {
            into.putInt(from.hashCode());
          }
        }, 1000, 0.03D, strategy());

    assertTrue(filter.add(new SuperClass()));
    assertTrue(filter.add(new SubClass()));
  }
}
