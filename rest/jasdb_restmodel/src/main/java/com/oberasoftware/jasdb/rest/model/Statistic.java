package com.oberasoftware.jasdb.rest.model;

/**
 * @author: renarj
 * Date: 1-5-12
 * Time: 21:44
 */
public class Statistic implements RestEntity {
    private String name;
    private long average;
    private long calls;
    private long lowest;
    private long highest;
    private long totalTime;

    public Statistic(String name, long average, long calls, long totalTime, long lowest, long highest) {
        this.name = name;
        this.average = average;
        this.calls = calls;
        this.totalTime = totalTime;
        this.lowest = lowest;
        this.highest = highest;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getAverage() {
        return average;
    }

    public void setAverage(long average) {
        this.average = average;
    }

    public long getCalls() {
        return calls;
    }

    public void setCalls(long calls) {
        this.calls = calls;
    }

    public long getTotalTime() {
        return totalTime;
    }

    public void setTotalTime(long totalTime) {
        this.totalTime = totalTime;
    }

    public long getLowest() {
        return lowest;
    }

    public void setLowest(long lowest) {
        this.lowest = lowest;
    }

    public long getHighest() {
        return highest;
    }

    public void setHighest(long highest) {
        this.highest = highest;
    }
}
