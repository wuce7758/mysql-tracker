package dbsync.event;

import dbsync.LogBuffer;

/**
 * Log row insertions and updates. The dbsync.event contain several insert/update rows
 * for a table. Note that each dbsync.event contains only rows for one table.
 * 
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class WriteRowsLogEvent extends RowsLogEvent
{
    public WriteRowsLogEvent(LogHeader header, LogBuffer buffer,
            FormatDescriptionLogEvent descriptionEvent)
    {
        super(header, buffer, descriptionEvent);
    }
}
