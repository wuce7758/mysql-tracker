package monitor;

import dbsync.LogEvent;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by hp on 14-9-23.
 */
public class TrackerMonitor {

    public Date startTimeDate;

    public Date endTimeDate;

    public int inEventNum;

    public int outEventNum;

    public int inSizeEvents;

    public int outSizeEvents;

    public long startDealTime;

    public long endDealTime;

    public long duringDealTime;

    public List<LogEvent> inEventList;

    public List<LogEvent> outEventList;

    public TrackerMonitor() {
        startTimeDate = null;
        endTimeDate = null;
        inEventNum = outEventNum = inSizeEvents = outSizeEvents =  0;
        duringDealTime = startDealTime = endDealTime = 0L;
        inEventList = new ArrayList<LogEvent>();
        outEventList = new ArrayList<LogEvent>();
    }

    public void clear() {
        startTimeDate = null;
        endTimeDate = null;
        inEventNum = outEventNum = inSizeEvents = outSizeEvents =  0;
        duringDealTime = startDealTime = endDealTime = 0L;
        inEventList = new ArrayList<LogEvent>();
        outEventList = new ArrayList<LogEvent>();
    }

}
