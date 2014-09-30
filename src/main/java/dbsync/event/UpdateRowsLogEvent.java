package dbsync.event;

import dbsync.LogBuffer;

/**
 * Log row updates with a before image. The dbsync.event contain several update rows
 * for a table. Note that each dbsync.event contains only rows for one table.
 * 
 * Also note that the row data consists of pairs of row data: one row for the
 * old data and one row for the new data.
 * 
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class UpdateRowsLogEvent extends RowsLogEvent
{
    public UpdateRowsLogEvent(LogHeader header, LogBuffer buffer,
            FormatDescriptionLogEvent descriptionEvent)
    {
        super(header, buffer, descriptionEvent);
    }
}
