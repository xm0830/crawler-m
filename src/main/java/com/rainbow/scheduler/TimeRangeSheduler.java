package com.rainbow.scheduler;

import com.rainbow.common.Prediction;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by xuming on 2017/4/28.
 */
public class TimeRangeSheduler implements Scheduler {
    @Override
    public boolean schedule(ScheduleRule... rules) {
        Prediction.predictNotNull(rules);

        Date now = new Date();
        String time = new SimpleDateFormat("HHmm").format(now);

        for (ScheduleRule rule : rules) {
            String startTime = ((TimeRangeRule) rule).startTime;
            String endTime = ((TimeRangeRule) rule).endTime;
            if (time.compareTo(startTime) >= 0 && time.compareTo(endTime) < 0) {
                return true;
            }
        }

        return false;
    }
}
