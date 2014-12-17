package mysql.dbsync.event;

import mysql.dbsync.LogBuffer;

/**
 * Log row deletions. The mysql.dbsync.event contain several delete rows for a table. Note
 * that each mysql.dbsync.event contains only rows for one table.
 * 
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class DeleteRowsLogEvent extends RowsLogEvent
{
    public DeleteRowsLogEvent(LogHeader header, LogBuffer buffer,
            FormatDescriptionLogEvent descriptionEvent)
    {
        super(header, buffer, descriptionEvent);
    }
}
