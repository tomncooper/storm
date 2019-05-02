package org.apache.storm.metric.internal;

import java.util.Arrays;

public class LastKCounter {

    private long[] counts;
    private int size;
    private int head;
    private boolean saturated;
    private long sample;

    public LastKCounter(int size) {

        this.size = size;
        counts = new long[size];
        head = 0;
        saturated = false;
        sample = 1;
    }

    public synchronized void addCount(long count){
        // Do not add zero counts (we do not expect these) as these are used as a test for if a cell in the array has
        // been filled in the statistics methods
        if (count > 0) {
            counts[head] = count;

            // Indicate if the buffer is now saturated (contains the full K entries)
            if (head == size - 1){
                saturated = true;
            }

            // Always wrap around to the beginning
            head = (head + 1) % size;

            // Update the sample to be the last seen value
            sample = count;
        }
    }

    private double calculate_median(){

        long[] buffer_copy;

        if (!saturated) {

            // Copy from the start to head - 1 (as head is always advanced after an call to add)
            // copyOfRange third argument is exclusive
            buffer_copy = Arrays.copyOfRange(counts, 0, head);

        } else {

            // Copy the whole counts buffer as it is full.
            buffer_copy = Arrays.copyOf(counts, counts.length);
        }

        Arrays.sort(buffer_copy);


        if (buffer_copy.length % 2 == 0){

            // If the buffer length is even

            int upper = buffer_copy.length / 2;

            return ((buffer_copy[upper - 1] + buffer_copy[upper]) / 2.0);


        } else {

            // If the buffer length is odd

            int middle = (int) Math.floor(buffer_copy.length / 2.0);

            return (double) buffer_copy[middle];

        }

    }

    private double calculate_std(double mean, int N, int stop){

        double numerator = 0.0;

        for (int i = 0; i <= stop; i++) {
            numerator += Math.pow((counts[i] - mean), 2);
        }

        return Math.sqrt(numerator/N);

    }

    public synchronized LastKResults get_stats() {

        if (!saturated && head == 0){
            return new LastKResults(0,0,0,0,0);
        }

        double total = 0.0;

        long max = 0;
        long min = Long.MAX_VALUE;

        int N;
        int stop;

        if (!saturated){

            N = head;
            stop = head - 1;

        } else {

            N = size;
            stop = size-1;
        }

        for (int i = 0; i <= stop; i++) {

            long count = counts[i];
            total += count;

            if (count > max){
                max = count;
            }

            if (count < min) {
                min = count;
            }
        }

        double mean = total / N;

        double std = calculate_std(mean, N, stop);

        double median = calculate_median();

        LastKResults results = new LastKResults(mean, median, std, max, min);

        return results;
    }

    public long getSample(){
        return sample;
    }
}


