package com.pxene.scheduler;

/**
 * Created by xuming on 2017/4/28.
 */
public interface Scheduler {
    boolean schedule(ScheduleRule... rules);
}
