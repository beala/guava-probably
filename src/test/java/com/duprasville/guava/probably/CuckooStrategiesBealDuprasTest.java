package com.duprasville.guava.probably;

import com.google.common.math.LongMath;
import org.junit.Test;

import java.math.RoundingMode;
import java.util.Random;

import static junit.framework.Assert.assertEquals;

public class CuckooStrategiesBealDuprasTest extends AbstractCuckooStrategiesTest {
    public AbstractCuckooStrategy strategy() {
        return CuckooStrategies.MURMUR128_BEALDUPRAS_32.strategy();
    }

    @Test
    public void altIndexIsReversible() throws Exception {
        // Largest power of two (must be even)
        final long max = Long.MAX_VALUE - 1; // must be even!!
        final long incr = 1000000L;
        final Random random = new Random(1L);
        final byte[] fingerprint = new byte[1];

        for (long index = 0; index != next(index, incr, max); index = next(index, incr, max)) {
            random.nextBytes(fingerprint);
            int f = (random.nextInt(126) + 1) * (random.nextBoolean() ? 1 : -1);
            final long altIndex = strategy().altIndex(index, f, max);
            final long altAltIndex = strategy().altIndex(altIndex, f, max);
            assertEquals("index should equal altIndex(altIndex(index)):" + f, index, altAltIndex);
        }
    }
}
