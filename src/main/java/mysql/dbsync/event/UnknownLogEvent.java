package mysql.dbsync.event;

import mysql.dbsync.LogEvent;

/**
 * Unknown_log_event
 * 
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class UnknownLogEvent extends LogEvent
{
    public UnknownLogEvent(LogHeader header)
    {
        super(header);
    }
}
