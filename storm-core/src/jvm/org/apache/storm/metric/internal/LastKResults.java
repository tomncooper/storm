package org.apache.storm.metric.internal;

public class LastKResults {

    private double mean;
    private double median;
    private double std;
    private long max;
    private long min;

    public LastKResults(double mean, double median, double std, long max, long min){

        this.mean = mean;
        this.median = median;
        this.std = std;
        this.max = max;
        this.min = min;

    }

    public double getMean() {
        return mean;
    }

    public double getMedian() {
        return median;
    }

    public double getStandardDeviation(){
        return std;
    }

    public long getMax() {
        return max;
    }

    public long getMin() {
        return min;
    }


}
