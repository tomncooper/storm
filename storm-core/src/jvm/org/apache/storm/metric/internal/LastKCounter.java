package org.apache.storm.metric.internal;

public class LastKCounter {

    long[] counts;
    int size;
    int head;
    long max;
    long min;

    public LastKCounter(int size) {

        this.size = size;
        counts = new long[size];
        head = 0;
        min = Long.MAX_VALUE;
    }

    public void addCount(long count){
        // Do not add zero counts (we do not expect these) as these are used as a test for if a cell in the array has
        // been filled in the statistics methods
        if (count > 0) {
            counts[head] = count;
            // Always wrap around to the beginning
            head = (head + 1) % size;

            if (count > max) {
                max = count;
            }

            if (count < min) {
                min = count;
            }
        }
    }

    public double mean() {

        double total = 0.0;
        double filledCells = 0.0;
        for (long count: counts) {
            if (count >0) {
                total += count;
                filledCells ++;
            }
        }
        return total / filledCells;
    }

    public long getMax() {
        return max;
    }

    public long getMin() {
        return min;
    }
}
