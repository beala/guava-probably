package com.duprasville.guava.probably;

import com.google.common.hash.Funnels;
import org.junit.Test;

import static com.google.common.base.Charsets.UTF_8;
import static junit.framework.Assert.assertEquals;

public class CuckooFilterProbabilisticFilterBealDuprasStrategyTest extends CuckooFilterProbabilisticFilterTest {
    @Override
    ProbabilisticFilter<CharSequence> filter(int capacity, double fpp) {
        return CuckooFilter.create(
                Funnels.stringFunnel(UTF_8),
                capacity,
                fpp,
                CuckooStrategies.MURMUR128_BEALDUPRAS_32.strategy());
    }

    @Override
    @Test
    public void capacity() {
        assertEquals(1000007, filter.capacity());
        assertEquals(1000003, filter(1000003, 0.9D).capacity());

        assertEquals(7, tinyFilter.capacity());
        assertEquals(3, filter(3, 0.9D).capacity());
    }
}
