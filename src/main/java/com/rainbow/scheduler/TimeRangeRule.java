package com.rainbow.scheduler;

/**
 * Created by xuming on 2017/4/28.
 */
public class TimeRangeRule implements ScheduleRule {
    public String startTime = null;
    public String endTime = null;

    public TimeRangeRule(String startTime, String endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
    }
}
