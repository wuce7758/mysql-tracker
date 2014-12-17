package mysql.dbsync.event.mariadb;

import mysql.dbsync.LogBuffer;
import mysql.dbsync.event.FormatDescriptionLogEvent;
import mysql.dbsync.event.IgnorableLogEvent;
import mysql.dbsync.event.LogHeader;

/**
 * mariadb10的BINLOG_CHECKPOINT_EVENT类型
 * 
 * @author jianghang 2014-1-20 下午2:22:04
 * @since 1.0.17
 */
public class BinlogCheckPointLogEvent extends IgnorableLogEvent {

    public BinlogCheckPointLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header, buffer, descriptionEvent);
        // do nothing , just mariadb binlog checkpoint
    }

}
