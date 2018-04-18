package com.duprasville.guava.probably;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

public class CuckooFilterBealDupras32Test extends AbstractCuckooFilterTest {
  AbstractCuckooStrategy strategy() {
    return CuckooStrategies.MURMUR128_BEALDUPRAS_32.strategy();
  }

  @Test
  public void createAndCheckCuckooFilterWithKnownFalsePositivesBealDupras32() {
    createAndCheckCuckooFilterWithKnownFalsePositives(
            ImmutableSet.of(217, 329, 581, 707, 757, 805, 863),
            25926
    );
  }

  @Test
  public void createAndCheckCuckooFilterWithKnownUtf8FalsePositivesBealDupras32() {
    createAndCheckCuckooFilterWithKnownUtf8FalsePositives(
            ImmutableSet.of(5, 315, 389, 443, 445, 615, 621, 703, 789, 861, 899),
            26610
    );
  }
}
