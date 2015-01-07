package monitor;

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

    public TrackerMonitor() {
        fetchStart = fetchEnd = persistenceStart = persistenceEnd = 0;
        perMinStart = perMinEnd = hbaseReadStart = hbaseReadEnd = 0;
        hbaseWriteStart = hbaseWriteEnd = serializeStart = serializeEnd = 0;
        fetchNum = persisNum = batchSize = 0;
        fetcherStart = fetcherEnd = decodeStart = decodeEnd = 0;
        sendStart = sendEnd = 0;
        delayTime = 0;
    }

    public void clear() {
        fetchStart = fetchEnd = persistenceStart = persistenceEnd = 0;
        perMinStart = perMinEnd = hbaseReadStart = hbaseReadEnd = 0;
        hbaseWriteStart = hbaseWriteEnd = serializeStart = serializeEnd = 0;
        fetchNum = persisNum = batchSize = 0;
        fetcherStart = fetcherEnd = decodeStart = decodeEnd = 0;
        sendStart = sendEnd = 0;
        delayTime = 0;
    }

}
