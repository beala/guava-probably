package com.duprasville.guava.probably;

import com.google.common.hash.Funnels;
import org.junit.Test;

import static com.google.common.base.Charsets.UTF_8;
import static junit.framework.Assert.assertEquals;

public class CuckooFilterProbabilisticFilterOriginalStrategyTest extends CuckooFilterProbabilisticFilterTest {
    @Override
    ProbabilisticFilter<CharSequence> filter(int capacity, double fpp) {
        return CuckooFilter.create(
                Funnels.stringFunnel(UTF_8),
                capacity,
                fpp,
                CuckooStrategies.ORIGINAL.strategy());
    }

    @Override
    @Test
    public void capacity() {
        assertEquals(1001390, filter.capacity());
        assertEquals(1761607, filter(1000003, 0.9D).capacity());

        assertEquals(7, tinyFilter.capacity());
        assertEquals(3, filter(3, 0.9D).capacity());
    }

}
