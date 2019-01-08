package org.apache.storm.metric.internal;

import junit.framework.TestCase;
import org.junit.Test;

import org.apache.storm.metric.internal.LastKCounter;

public class LastKCounterTest extends TestCase {

    @Test
    public void testMeanFull() {
        LastKCounter counter = new LastKCounter(10);

        long[] counts = {5, 4, 3, 2, 1, 5, 4 ,3 ,2, 1};
        for (long count: counts) {
            counter.addCount(count);
        }

        assertEquals(3.0, counter.mean());
    }

    @Test
    public void testMeanNotFull() {
        LastKCounter counter = new LastKCounter(10);

        long[] counts = {5, 4, 3, 2, 1, 5};
        for (long count: counts) {
            counter.addCount(count);
        }

        assertEquals((20.0 / 6.0), counter.mean());
    }

    @Test
    public void testMax() {
        LastKCounter counter = new LastKCounter(10);

        long[] counts = {5, 4, 3, 2, 1, 5, 4 ,3 ,2, 1};
        for (long count: counts) {
            counter.addCount(count);
        }

        assertEquals(5, counter.getMax());
    }

    @Test
    public void testMin() {
        LastKCounter counter = new LastKCounter(10);

        long[] counts = {5, 4, 3, 2, 1, 5, 4 ,3 ,2, 1};
        for (long count: counts) {
            counter.addCount(count);
        }

        assertEquals(1, counter.getMin());
    }
}

