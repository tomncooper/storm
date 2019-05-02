package org.apache.storm.metric.internal;

import junit.framework.TestCase;
import org.junit.Test;

import org.apache.storm.metric.internal.LastKCounter;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class LastKCounterTest extends TestCase {

    @Test
    public void testEmpty() {

        LastKCounter counter = new LastKCounter(10);
        LastKResults results = counter.get_stats();

        assertEquals(0.0, results.getMean());
        assertEquals(0.0, results.getMedian());
        assertEquals(0.0, results.getStandardDeviation());
        assertEquals(0L, results.getMax());
        assertEquals(0L, results.getMin());
    }

    @Test
    public void testMeanFull() {
        LastKCounter counter = new LastKCounter(10);

        long[] counts = {5, 4, 3, 2, 1, 5, 4 ,3 ,2, 1};
        for (long count: counts) {
            counter.addCount(count);
        }

        LastKResults results = counter.get_stats();

        assertEquals(3.0, results.getMean());
    }

    @Test
    public void testMeanOverFull() {
        LastKCounter counter = new LastKCounter(10);

        long[] counts = {5, 4, 3, 2, 1, 5, 4 ,3 ,2, 1, 10, 8, 6, 4, 2, 10, 8 ,6 ,4, 2};
        for (long count: counts) {
            counter.addCount(count);
        }

        LastKResults results = counter.get_stats();

        assertEquals(6.0, results.getMean());
    }

    @Test
    public void testMeanNotFull() {
        LastKCounter counter = new LastKCounter(10);

        long[] counts = {5, 4, 3, 2, 1, 5};
            for (long count: counts) {
            counter.addCount(count);
        }

        LastKResults results = counter.get_stats();

        assertEquals((20.0 / 6.0), results.getMean());
    }

    @Test
    public void testMedianFull() {
        LastKCounter counter = new LastKCounter(10);

        long[] counts = {5, 4, 3, 2, 1, 5, 4 ,3 ,2, 1};
        for (long count: counts) {
            counter.addCount(count);
        }

        LastKResults results = counter.get_stats();

        assertEquals(3.0, results.getMedian());
    }

    @Test
    public void testMedianNotFullOdd() {
        LastKCounter counter = new LastKCounter(10);

        long[] counts = {5, 4, 3, 2, 1};
        for (long count: counts) {
            counter.addCount(count);
        }

        LastKResults results = counter.get_stats();

        assertEquals(3.0, results.getMedian());
    }

    @Test
    public void testMedianNotFullEven() {
        LastKCounter counter = new LastKCounter(10);

        long[] counts = {5, 4, 3, 2, 1, 5, 3, 10};
        for (long count: counts) {
            counter.addCount(count);
        }

        LastKResults results = counter.get_stats();

        assertEquals(3.5, results.getMedian());
    }

    @Test
    public void testMedianOverFull() {
        LastKCounter counter = new LastKCounter(10);

        long[] counts = {5, 4, 3, 2, 1, 5, 4 ,3 ,2, 1, 10, 8, 6, 4, 2, 10, 8 ,6 ,4, 2};
        for (long count: counts) {
            counter.addCount(count);
        }

        LastKResults results = counter.get_stats();

        assertEquals(6.0, results.getMedian());
    }

    @Test
    public void testMax() {
        LastKCounter counter = new LastKCounter(10);

        long[] counts = {5, 4, 3, 2, 1, 5, 4 ,3 ,2, 1};
        for (long count: counts) {
            counter.addCount(count);
        }

        LastKResults results = counter.get_stats();

        assertEquals(5, results.getMax());
    }

    @Test
    public void testMin() {
        LastKCounter counter = new LastKCounter(10);

        long[] counts = {5, 4, 3, 2, 1, 5, 4 ,3 ,2, 1};
        for (long count: counts) {
            counter.addCount(count);
        }

        LastKResults results = counter.get_stats();

        assertEquals(1, results.getMin());
    }

    @Test
    public void testSTDFull() {
        LastKCounter counter = new LastKCounter(10);

        long[] counts = {5, 4, 3, 2, 1, 5, 4 ,3 ,2, 1};
        for (long count: counts) {
            counter.addCount(count);
        }

        LastKResults results = counter.get_stats();

        double std = new BigDecimal(results.getStandardDeviation()).setScale(3, RoundingMode.HALF_UP).doubleValue();

        assertEquals(1.414, std);
    }

    @Test
    public void testSTDNotFull() {
        LastKCounter counter = new LastKCounter(10);

        long[] counts = {5, 4, 3, 2, 1, 5, 3};
        for (long count: counts) {
            counter.addCount(count);
        }

        LastKResults results = counter.get_stats();

        double std = new BigDecimal(results.getStandardDeviation()).setScale(3, RoundingMode.HALF_UP).doubleValue();

        assertEquals(1.385, std);
    }
}

