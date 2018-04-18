package com.duprasville.guava.probably;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

public class CuckooFilterOriginalTest extends AbstractCuckooFilterTest {
  AbstractCuckooStrategy strategy() {
    return CuckooStrategies.ORIGINAL.strategy();
  }

  @Test
  public void createAndCheckCuckooFilterWithKnownFalsePositivesOriginal() {
    createAndCheckCuckooFilterWithKnownFalsePositives(
            ImmutableSet.of(161, 315, 665, 803, 823),
            15167
    );
  }

  @Test
  public void createAndCheckCuckooFilterWithKnownUtf8FalsePositivesOriginal() {
    createAndCheckCuckooFilterWithKnownUtf8FalsePositives(
            ImmutableSet.<Integer>of(61, 443, 653, 691, 777),
            15274
    );
  }
}
