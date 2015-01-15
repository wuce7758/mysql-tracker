package monitor;

import monitor.constants.JDMysqlTrackerMonitorType;
import monitor.constants.JDMysqlTrackerPhenix;
import net.sf.json.JSONObject;
import protocol.json.JSONConvert;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by hp on 14-9-23.
 */
public class TrackerMonitor {

    public long fetchStart;

    public long fetchEnd;

    public long persistenceStart;

    public long persistenceEnd;

    public long sendStart;

    public long sendEnd;

    public long perMinStart;

    public long perMinEnd;

    public long hbaseReadStart;

    public long hbaseReadEnd;

    public long hbaseWriteStart;

    public long hbaseWriteEnd;

    public long serializeStart;

    public long serializeEnd;

    public long fetchNum;

    public long persisNum;

    public long batchSize;//bytes for unit

    public long fetcherStart;

    public long fetcherEnd;

    public long decodeStart;

    public long decodeEnd;

    public long delayTime;

    public String exMsg;

    public TrackerMonitor() {
        fetchStart = fetchEnd = persistenceStart = persistenceEnd = 0;
        perMinStart = perMinEnd = hbaseReadStart = hbaseReadEnd = 0;
        hbaseWriteStart = hbaseWriteEnd = serializeStart = serializeEnd = 0;
        fetchNum = persisNum = batchSize = 0;
        fetcherStart = fetcherEnd = decodeStart = decodeEnd = 0;
        sendStart = sendEnd = 0;
        delayTime = 0;
        exMsg = "";
    }

    public void clear() {
        fetchStart = fetchEnd = persistenceStart = persistenceEnd = 0;
        perMinStart = perMinEnd = hbaseReadStart = hbaseReadEnd = 0;
        hbaseWriteStart = hbaseWriteEnd = serializeStart = serializeEnd = 0;
        fetchNum = persisNum = batchSize = 0;
        fetcherStart = fetcherEnd = decodeStart = decodeEnd = 0;
        sendStart = sendEnd = 0;
        delayTime = 0;
        exMsg = "";
    }

    public JrdwMonitorVo toJrdwMonitor(int id) {
        JrdwMonitorVo jmv = new JrdwMonitorVo();
        jmv.setId(id);
        //pack the member / value  to the Map<String,String> or Map<String,Long>
        Map<String, Long> content = new HashMap<String, Long>();
        switch (id) {
            case JDMysqlTrackerPhenix.FETCH_MONITOR:
                content.put(JDMysqlTrackerPhenix.FETCH_ROWS, fetchNum);
                content.put(JDMysqlTrackerPhenix.FETCH_SIZE, batchSize);
                break;
            case JDMysqlTrackerPhenix.PERSIS_MONITOR:
                content.put(JDMysqlTrackerPhenix.SEND_ROWS, persisNum);
                content.put(JDMysqlTrackerPhenix.SEND_SIZE, batchSize);
                content.put(JDMysqlTrackerPhenix.SEND_TIME, (sendEnd - sendStart));
                content.put(JDMysqlTrackerPhenix.DELAY_TIME, delayTime);
                break;
            default:
                break;
        }
        JSONObject jo = JSONConvert.MapToJson(content);
        jmv.setContent(jo.toString());
        return jmv;
    }

    public JrdwMonitorVo toJrdwMonitor(int id, String jobId) {
        JrdwMonitorVo jmv = new JrdwMonitorVo();
        jmv.setId(id);
        jmv.setJrdw_mark(jobId);
        //pack the member / value  to the Map<String,String> or Map<String,Long>
        Map<String, Long> content = new HashMap<String, Long>();
        switch (id) {
            case JDMysqlTrackerPhenix.FETCH_MONITOR:
                content.put(JDMysqlTrackerPhenix.FETCH_ROWS, fetchNum);
                content.put(JDMysqlTrackerPhenix.FETCH_SIZE, batchSize);
                break;
            case JDMysqlTrackerPhenix.PERSIS_MONITOR:
                content.put(JDMysqlTrackerPhenix.SEND_ROWS, persisNum);
                content.put(JDMysqlTrackerPhenix.SEND_SIZE, batchSize);
                content.put(JDMysqlTrackerPhenix.SEND_TIME, (sendEnd - sendStart));
                content.put(JDMysqlTrackerPhenix.DELAY_TIME, delayTime);
                break;
            default:
                break;
        }
        //map to json
        JSONObject jo = JSONConvert.MapToJson(content);
        jmv.setContent(jo.toString());
        return jmv;
    }

    public JrdwMonitorVo toJrdwMonitorOnline(int id, String jobId) {
        JrdwMonitorVo jmv = new JrdwMonitorVo();
        jmv.setId(id);
        jmv.setJrdw_mark(jobId);
        //pack the member / value  to the Map<String,String> or Map<String,Long>
        Map<String, Long> content = new HashMap<String, Long>();
        Map<String, String> msgContent = new HashMap<String, String>();
        JSONObject jo;
        switch (id) {
            case JDMysqlTrackerMonitorType.FETCH_MONITOR:
                content.put(JDMysqlTrackerMonitorType.FETCH_ROWS, fetchNum);
                content.put(JDMysqlTrackerMonitorType.FETCH_SIZE, batchSize);
                jo = JSONConvert.MapToJson(content);
                break;
            case JDMysqlTrackerMonitorType.PERSIS_MONITOR:
                content.put(JDMysqlTrackerPhenix.SEND_ROWS, persisNum);
                content.put(JDMysqlTrackerPhenix.SEND_SIZE, batchSize);
                content.put(JDMysqlTrackerPhenix.SEND_TIME, (sendEnd - sendStart));
                content.put(JDMysqlTrackerPhenix.DELAY_TIME, delayTime);
                jo = JSONConvert.MapToJson(content);
                break;
            case JDMysqlTrackerMonitorType.EXCEPTION_MONITOR:
                msgContent.put(JDMysqlTrackerMonitorType.EXCEPTION, exMsg);
                jo = JSONConvert.MapToJson(msgContent);
                break;
            default:
                jo = new JSONObject();
                break;
        }
        //map to json
        jmv.setContent(jo.toString());
        return jmv;
    }


}
