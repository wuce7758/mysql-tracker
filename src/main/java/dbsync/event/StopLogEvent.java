package dbsync.event;

import dbsync.LogBuffer;
import dbsync.LogEvent;

/**
 * Stop_log_event.
 * 
 * The Post-Header and Body for this dbsync.event type are empty; it only has the
 * Common-Header.
 * 
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class StopLogEvent extends LogEvent
{
    public StopLogEvent(LogHeader header, LogBuffer buffer,
            FormatDescriptionLogEvent description_event)
    {
        super(header);
    }
}
