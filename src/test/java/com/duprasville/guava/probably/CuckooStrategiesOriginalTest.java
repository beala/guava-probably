package com.duprasville.guava.probably;

import com.google.common.math.LongMath;
import org.junit.Test;

import java.util.Random;

import static junit.framework.Assert.assertEquals;

public class CuckooStrategiesOriginalTest extends AbstractCuckooStrategiesTest {

    public AbstractCuckooStrategy strategy() {
        return CuckooStrategies.ORIGINAL.strategy();
    }

    @Test
    public void altIndexIsReversible() throws Exception {
        int ALT_INDEX_TEST_ROUNDS = 5;

        final Random random = new Random(); //(1L);

        final long incr = 1000000L;
        final byte[] fingerprint = new byte[1];

        for(int rounds = 0; rounds < ALT_INDEX_TEST_ROUNDS; rounds++) {
            final long max = randomPowerOfTwo(random);
            for (long index = 0; index != next(index, incr, max); index = next(index, incr, max)) {
                random.nextBytes(fingerprint);
                int f = (random.nextInt(126) + 1) * (random.nextBoolean() ? 1 : -1);
                final long altIndex = strategy().altIndex(index, f, max);
                final long altAltIndex = strategy().altIndex(altIndex, f, max);
                assertEquals("index should equal altIndex(altIndex(index)):" + f, index, altAltIndex);
            }
        }
    }

    private long randomPowerOfTwo(Random random) {
        // 62 is the largest safe exponent. Larger values will overflow.
        // 63 because bound on nextInt is exclusive.
        return LongMath.pow(2, random.nextInt(63));
    }
}
