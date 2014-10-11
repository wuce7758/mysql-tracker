package com.jd.bdp.mysql.tracker;

import com.jd.bdp.magpie.MagpieExecutor;
import dbsync.DirectLogFetcherChannel;
import dbsync.LogContext;
import dbsync.LogDecoder;
import dbsync.LogEvent;
import dbsync.event.QueryLogEvent;
import driver.MysqlConnector;
import driver.MysqlQueryExecutor;
import driver.MysqlUpdateExecutor;
import driver.packets.HeaderPacket;
import driver.packets.client.BinlogDumpCommandPacket;
import driver.packets.server.ResultSetPacket;
import driver.utils.PacketManager;
import monitor.MonitorToKafkaProducer;
import monitor.MonitorToWhaleConsumer;
import monitor.MonitorToWhaleProducer;
import monitor.TrackerMonitor;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import tracker.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by hp on 14-9-22.
 */
public class Handler1 implements MagpieExecutor {


    //tracker's log
    private Logger logger = LoggerFactory.getLogger(Handler1.class);

    //mysql JDBC by socket
    private MysqlConnector connector;

    private MysqlQueryExecutor queryExecutor ;

    private MysqlUpdateExecutor updateExecutor ;

    //get the table structure
    private MysqlConnector connectorTable;

    //table meta cache
    private TableMetaCache tableMetaCache;

    //log event convert
    LogEventConvert eventParser;

    //configuration for tracker of mysql
    private TrackerConfiger configer ;

    //entry position, manager the offset, for file
    private EntryPosition startPosition ;

    //HBase Operator
    private HBaseOperator hbaseOP;

    // batch size threshold for per fetch the number of the event,if event size >= batchsize then
    // bigFetch() return
    // now by the py test we set the var is 1000

    private int batchsize = 3000;

    // time threshold if batch size number is not reached then if the time is
    // now by the py test we set the var is 1.5 second

    private double secondsize = 1.5;

    //per second write the position
    private int secondPer = 60;

    //multi Thread share queue
    private BlockingQueue<LogEvent> eventQueue ;

    //Global variables
    private LogEvent globalEvent = null;

    private LogEvent globalXidEvent = null;

    private String globalBinlogName = null;

    private byte[] globalEventRowKey = null;

    private byte[] globalXidEventRowKey = null;

    private byte[] globalEntryRowKey = null;

    //control variable
    private boolean running;

    private long startTime;

    //run control
    private List<LogEvent> eventList;

    //fetchMonitor
    private TrackerMonitor fetchMonitor;
    private TrackerMonitor persistenceMonitor;
    //private MonitorToWhaleProducer whaleMonitorProducer;
    //private MonitorToWhaleConsumer whaleMonitorConsumer;
    private MonitorToKafkaProducer kafkaMonitorProducer;


    //multiple thread


    //constructor
    public Handler1(TrackerConfiger configer) {
        this.configer = configer;
    }

    public Handler1(String username, String password, String address, int port, Long slaveId) {
        configer = new TrackerConfiger(username, password, address, port, slaveId);
    }

    public Handler1(String username, String password, String address, int port, Long slaveId, String hbase) {
        configer = new TrackerConfiger(username, password, address, port, slaveId, hbase);
    }

    public Handler1() {
        try {
            InputStream in = new BufferedInputStream(new FileInputStream("conf/tracker.properties"));
            Properties pro = new Properties();
            pro.load(in);
            String sumAddress = pro.getProperty("mysql.address");
            String[] subStrings = sumAddress.split(":");
            String address =subStrings[0];
            int port = Integer.parseInt(subStrings[1]);
            String username = pro.getProperty("mysql.usr");
            String password = pro.getProperty("mysql.psd");
            Long slaveId = Long.valueOf(pro.getProperty("mysql.slaveId"));
            String hbase = pro.getProperty("hbase.rootdir");
            configer = new TrackerConfiger(username, password, address, port, slaveId, hbase);
        } catch (Exception e) {
            logger.error("load the mysql conf properties failed!!!");
            e.printStackTrace();
        }
    }




    public void prepare(String id) throws Exception {

        //initialize config
        //configer = new TrackerConfiger("canal","canal","192.168.213.41",3306,Long.valueOf(7777));

        //log comment
        logger.info("starting the  tracker ......");
        //initialize the connector and executor
        boolean mysqlExist = true;
        do {
            connector = new MysqlConnector(new InetSocketAddress(configer.getAddress(), configer.getPort()),
                    configer.getUsername(),
                    configer.getPassword());
            connectorTable = new MysqlConnector(new InetSocketAddress(configer.getAddress(), configer.getPort()),
                    configer.getUsername(),
                    configer.getPassword());
            //connect mysql to find start position and dump binlog
            try {
                connector.connect();
                connectorTable.connect();
                mysqlExist = true;
            } catch (IOException e) {
                logger.error("connector connect failed or connectorTable connect failed");
                e.printStackTrace();
                logger.error("the mysql " + configer.getAddress() + " is not available ...");
                mysqlExist = false;
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException ei) {
                    ei.printStackTrace();
                }
            }
        } while(!mysqlExist);
        queryExecutor = new MysqlQueryExecutor(connector);
        updateExecutor = new MysqlUpdateExecutor(connector);
        //hbase operator
        hbaseOP = new HBaseOperator(configer.getAddress() + ":" + configer.getPort() +
        "," + configer.getHbaseString());
        hbaseOP.getConf().set("hbase.rootdir","hdfs://localhost:9000/hbase");
        hbaseOP.getConf().set("hbase.cluster.distributed","true");
        hbaseOP.getConf().set("hbase.zookeeper.quorum","localhost");
        hbaseOP.getConf().set("hbase.zookeeper.property.clientPort","2181");
        hbaseOP.getConf().set("dfs.socket.timeout", "180000");
        //load from properties
        try {
            InputStream in = new BufferedInputStream(new FileInputStream("conf/tracker.properties"));
            Properties pro = new Properties();
            pro.load(in);
            if(!pro.getProperty("hbase.rootdir").equals(""))
                hbaseOP.getConf().set("hbase.rootdir",pro.getProperty("hbase.rootdir"));
            if(!pro.getProperty("hbase.zookeeper.quorum").equals(""))
                hbaseOP.getConf().set("hbase.zookeeper.quorum",pro.getProperty("hbase.zookeeper.quorum"));
            if(!pro.getProperty("hbase.zookeeper.property.clientPort").equals(""))
                hbaseOP.getConf().set("hbase.zookeeper.property.clientPort",pro.getProperty("hbase.zookeeper.property.clientPort"));
            if(!pro.getProperty("dfs.socket.timeout").equals(""))
                hbaseOP.getConf().set("dfs.socket.timeout",pro.getProperty("dfs.socket.timeout"));
        } catch (Exception e) {
            logger.error("load the hbase conf properties failed!!!");
            e.printStackTrace();
        }
        //find start position
        //log comment
        logger.info("find start position");
        startPosition = findStartPosition();
        if(startPosition == null) throw new NullPointerException("start position is null");
        //get the table structure
        tableMetaCache = new TableMetaCache(connectorTable);
        //initialize the log event convert (to the entry)
        eventParser = new LogEventConvert();
        eventParser.setTableMetaCache(tableMetaCache);
        //queue
        eventQueue = new LinkedBlockingQueue<LogEvent>();

        //thread start
        //Thread : take the binlog data from the mysql
        logger.info("start the tracker thread to dump the binlog data from mysql...");
        FetchThread takeData = new FetchThread();
        takeData.start();
        //Thread :  per minute get the event
        logger.info("start the minute thread to save the position per minute as checkpoint...");
        PerminTimer minTask = new PerminTimer();
        Timer timer = new Timer();
        timer.schedule(minTask, 1000, secondPer * 1000);

        //run() control
        startTime = new Date().getTime();
        eventList = new ArrayList<LogEvent>();

        //fetchMonitor initialize
        fetchMonitor = new TrackerMonitor();
        persistenceMonitor = new TrackerMonitor();
//        whaleMonitorProducer = new MonitorToWhaleProducer();
//        whaleMonitorProducer.open();
//        whaleMonitorConsumer = new MonitorToWhaleConsumer();
//        whaleMonitorConsumer.open();
        kafkaMonitorProducer = new MonitorToKafkaProducer();
        kafkaMonitorProducer.open();

        //log
        logger.info("tracker is started successfully......");
    }


    //find start position include binlog file name and offset
    private EntryPosition findStartPosition()throws IOException{
        EntryPosition entryPosition;
        //load form file
        entryPosition = findHBaseStartPosition();
        if(entryPosition == null){
            //load from mysql
            logger.info("file position load failed , get the position from mysql!");
            entryPosition = findMysqlStartPosition();
        }
        else{
            logger.info("file position loaded!");
        }
        return(entryPosition);
    }

    //find position from HBase
    private EntryPosition findHBaseStartPosition() throws IOException{
        EntryPosition entryPosition = null;
        Get get = new Get(Bytes.toBytes(hbaseOP.trackerRowKey));
        get.addFamily(hbaseOP.getFamily());
        Result result = hbaseOP.getHBaseData(get,hbaseOP.getCheckpointSchemaName());
        for(KeyValue kv : result.raw()){
            byte[] value = kv.getValue();
            if(value != null) {
                String binXid = new String(value);
                if (binXid.contains(":")) {
                    String[] dataSplit = binXid.split(":");
                    entryPosition = new EntryPosition(dataSplit[0], Long.valueOf(dataSplit[1]));
                } else {
                    String stringValue = Bytes.toString(value);
                    Long longValue = Long.valueOf(stringValue);
                    globalEventRowKey = Bytes.toBytes(longValue);
                }
            }
        }
        return(entryPosition);
    }

    //find position by mysql
    private EntryPosition findMysqlStartPosition()throws IOException{
        ResultSetPacket resultSetPacket = queryExecutor.query("show master status");
        List<String> fields = resultSetPacket.getFieldValues();
        if(CollectionUtils.isEmpty(fields)){
            throw new NullPointerException("show master status failed!");
        }
        //binlogXid
        EntryPosition entryPosition = new EntryPosition(fields.get(0),Long.valueOf(fields.get(1)));
        //eventXid
        Long pos = 0L;
        globalEventRowKey = Bytes.toBytes(pos);
        return(entryPosition);
    }


    class PerminTimer extends TimerTask {

        private Logger logger = LoggerFactory.getLogger(PerminTimer.class);

        @Override
        public void run(){

            //monitor the mysql connection , is connection is invalid then close all thread


            if(globalBinlogName != null && globalXidEvent != null && globalXidEventRowKey != null) {
                Calendar cal = Calendar.getInstance();
                DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                String time = sdf.format(cal.getTime());
                String rowKey = hbaseOP.trackerRowKey + ":" + time;
                Put put = new Put(Bytes.toBytes(rowKey));
                String xidValue = globalBinlogName + ":" + globalXidEvent.getLogPos();
                Long xidEventRowLong = Bytes.toLong(globalXidEventRowKey);
                String xidEventRowString = String.valueOf(xidEventRowLong);
                put.add(hbaseOP.getFamily(), Bytes.toBytes(hbaseOP.binlogXidCol), Bytes.toBytes(xidValue));
                put.add(hbaseOP.getFamily(), Bytes.toBytes(hbaseOP.eventXidCol), Bytes.toBytes(xidEventRowString));
                try {
                    hbaseOP.putHBaseData(put, hbaseOP.getCheckpointSchemaName());
                } catch (IOException e) {
                    logger.error("per minute persistence failed!!!");
                    e.printStackTrace();
                }
                logger.info("per minute persistence the binlog xid and event xid to checkpoint... " +
                            ",row kye is " + rowKey +
                            ",col is :" + xidValue + "; col is :" + xidEventRowString);
            }
        }
    }

    //Thread : take the binlog data from the mysql
    class FetchThread extends Thread {

        //dbsync interface
        private DirectLogFetcherChannel fetcher;

        private LogDecoder decoder;

        private LogContext context;

        private Logger logger = LoggerFactory.getLogger(FetchThread.class);

        private boolean running = true;

        public void run()  {
            try {
                preRun();
                while(running) {
                    while (fetcher.fetch()) {
                        logger.info("fetch the binlog data (event) successfully...");
                        LogEvent event = decoder.decode(fetcher, context);
                        if (event == null) {
                            logger.error("fetched event is null!!!");
                            throw new NullPointerException("event is null!");
                        }
                        logger.info("---------------->get event : " +
                                        LogEvent.getTypeName(event.getHeader().getType()) +
                                        ",----> now pos: " +
                                        (event.getLogPos() - event.getEventLen()) +
                                        ",----> next pos: " +
                                        event.getLogPos() +
                                        ",----> binlog file : " +
                                        eventParser.getBinlogFileName()
                        );
                        if (event.getHeader().getType() == LogEvent.QUERY_EVENT) {
                            logger.info(",----> sql : " +
                                            ((QueryLogEvent) event).getQuery()
                            );
                        }
                        //System.out.println();
                        try {
                            if (event != null) eventQueue.put(event);
                        } catch (InterruptedException e) {
                            logger.error("eventQueue and entryQueue add data failed!!!");
                            throw new InterruptedIOException();
                        }
                        //fetchMonitor update
                        fetchMonitor.inEventNum++;
                        fetchMonitor.inSizeEvents += event.getEventLen(); //bit unit
                        if (fetchMonitor.inEventNum == 1) {
                            fetchMonitor.startDealTime = new Date().getTime();
                            fetchMonitor.startTimeDate = new Date();
                        }
                        if (fetchMonitor.inEventNum == batchsize ||
                                new Date().getTime() - fetchMonitor.startDealTime >= secondsize * 1000) {
                            fetchMonitor.endDealTime = new Date().getTime();
                            fetchMonitor.endTimeDate = new Date();
                            fetchMonitor.duringDealTime = fetchMonitor.endDealTime - fetchMonitor.startDealTime;
                            if (fetchMonitor.inEventNum > 0) {
                            /*logger.info(":::::::::::::::::::::::::::::::::::::::::::MONITOR : fetch data : count -> " +
                                            fetchMonitor.inEventNum +
                                            ",size(event length) -> " +
                                            fetchMonitor.inSizeEvents +
                                            ",start time -> " +
                                            fetchMonitor.startTimeDate +
                                            ",end time -> " +
                                            fetchMonitor.endTimeDate +
                                            ",during time -> " +
                                            fetchMonitor.duringDealTime
                            );*/
                                //whale monitor
                                try {
//                                whaleMonitorProducer.send(0, String.valueOf(fetchMonitor.inEventNum));
//                                whaleMonitorProducer.send(0, String.valueOf(fetchMonitor.inSizeEvents));
//                                whaleMonitorProducer.send(0, String.valueOf(fetchMonitor.startTimeDate));
//                                whaleMonitorProducer.send(0, String.valueOf(fetchMonitor.endTimeDate));
//                                whaleMonitorProducer.send(0, String.valueOf(fetchMonitor.duringDealTime));
                                    String key = "tracker:" + new Date();
                                    kafkaMonitorProducer.send(key, String.valueOf(fetchMonitor.inEventNum));
                                    kafkaMonitorProducer.send(key, String.valueOf(fetchMonitor.inSizeEvents));
                                    kafkaMonitorProducer.send(key, String.valueOf(fetchMonitor.startTimeDate));
                                    kafkaMonitorProducer.send(key, String.valueOf(fetchMonitor.endTimeDate));
                                    kafkaMonitorProducer.send(key, String.valueOf(fetchMonitor.duringDealTime));
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                            //after fetchMonitor
                            fetchMonitor.clear();
                        }
                    }
                    //fetch return false , and re connect to mysql
                    checkMysqlConn();
                    reConfig();
                }
            } catch (IOException e){
                logger.warn("fetch data failed!!! " + " Exception : " + e.getMessage());
                e.printStackTrace();
                //refind start position
                try {
                    startPosition = findMysqlStartPosition();
                } catch (IOException ei) {
                    logger.warn("refind position error!!! stop the all thread and fetch thread exit" + "," +
                            "exception is " + ei.getMessage());
                    ei.printStackTrace();
                    return;
                }
                FetchThread refetch = new FetchThread();
                refetch.start();
            }
            running = false;
        }

        public void preRun() throws IOException {
            //binlog dump thread configuration
            logger.info("set the binlog configuration for the binlog dump");
            updateExecutor.update("set wait_timeout=9999999");
            updateExecutor.update("set net_write_timeout=1800");
            updateExecutor.update("set net_read_timeout=1800");
            updateExecutor.update("set names 'binary'");//this will be my try to test no binary
            updateExecutor.update("set @master_binlog_checksum= '@@global.binlog_checksum'");
            updateExecutor.update("SET @mariadb_slave_capability='" + LogEvent.MARIA_SLAVE_CAPABILITY_MINE + "'");
            //send binlog dump packet and mysql will establish a binlog dump thread
            logger.info("send the binlog dump packet to mysql , let mysql set up a binlog dump thread in mysql");
            BinlogDumpCommandPacket binDmpPacket = new BinlogDumpCommandPacket();
            binDmpPacket.binlogFileName = startPosition.getJournalName();
            binDmpPacket.binlogPosition = startPosition.getPosition();
            binDmpPacket.slaveServerId = configer.getSlaveId();
            byte[] dmpBody = binDmpPacket.toBytes();
            HeaderPacket dmpHeader = new HeaderPacket();
            dmpHeader.setPacketBodyLength(dmpBody.length);
            dmpHeader.setPacketSequenceNumber((byte) 0x00);
            PacketManager.write(connector.getChannel(), new ByteBuffer[]{ByteBuffer.wrap(dmpHeader.toBytes()), ByteBuffer.wrap(dmpBody)});
            //initialize the dbsync to fetch the binlog data
            logger.info("initialize the dbsync class");
            fetcher = new DirectLogFetcherChannel(connector.getReceiveBufferSize());
            fetcher.start(connector.getChannel());
            decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
            context = new LogContext();
        }

        private void checkMysqlConn() {
            //if connection is aborted waiting and reconnect mysql
            do {
                logger.info("mysql connection is aborted ...");
                try {
                    connector.reconnect();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if(!connector.isConnected()) {
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } while(!connector.isConnected());
            do {
                logger.info("mysql(table meta) connection is aborted ...");
                try {
                    connectorTable.reconnect();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if(!connectorTable.isConnected()) {
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } while(!connectorTable.isConnected());
        }

        private void reConfig() throws IOException{
            startPosition = findStartPosition();
            //error here do not updateExecutor.update() second times
            //preRun();
            BinlogDumpCommandPacket binDmpPacket = new BinlogDumpCommandPacket();
            binDmpPacket.binlogFileName = startPosition.getJournalName();
            binDmpPacket.binlogPosition = startPosition.getPosition();
            binDmpPacket.slaveServerId = configer.getSlaveId();
            byte[] dmpBody = binDmpPacket.toBytes();
            HeaderPacket dmpHeader = new HeaderPacket();
            dmpHeader.setPacketBodyLength(dmpBody.length);
            dmpHeader.setPacketSequenceNumber((byte) 0x00);
            PacketManager.write(connector.getChannel(), new ByteBuffer[]{ByteBuffer.wrap(dmpHeader.toBytes()), ByteBuffer.wrap(dmpBody)});
            //initialize the dbsync to fetch the binlog data
            logger.info("initialize the dbsync class");
            fetcher = new DirectLogFetcherChannel(connector.getReceiveBufferSize());
            fetcher.start(connector.getChannel());
            decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
            context = new LogContext();
        }

    }


    public void reload(String id) {

    }


    public void pause(String id) throws Exception {

    }


    public void close(String id) throws Exception {

        connector.disconnect();
        connectorTable.disconnect();

//        whaleMonitorProducer.close();
//        whaleMonitorConsumer.close();
        kafkaMonitorProducer.close();
    }


    public void run() throws Exception {
        //logger.info("getting the queue data to the local list");
        //while + sleep
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            logger.error("sleep error");
            e.printStackTrace();
        }
        //take the data from the queue
        while(!eventQueue.isEmpty()) {
            try {
                LogEvent event = eventQueue.take();
                if(event!=null) eventList.add(event);
                //monitor update
                if(event != null) {
                    persistenceMonitor.outEventNum++;
                    persistenceMonitor.outSizeEvents += event.getEventLen();
                    if(persistenceMonitor.outEventNum == 1) {
                        persistenceMonitor.startDealTime = new Date().getTime();
                        persistenceMonitor.startTimeDate = new Date();
                    }
                }
                //per turn do not load much data
                if(eventList.size() >= batchsize) break;
            } catch (InterruptedException e){
                e.printStackTrace();
            }
        }
        //persistence the batch size event data to event table
        if ((eventList.size() >= batchsize || new Date().getTime() - startTime > secondsize * 1000) ) {
            try {
                //logger.info("persistence the entry list data to the local disk");
                //read the start pos by global and write the new start pos of event row key
                // for tracker to global
                writeHBaseEvent();
            } catch (Exception e){
                logger.error("persistence event list error");
                e.printStackTrace();
            }
            if(existXid(eventList)){
                //persistence xid pos of binlog and event row key to checkpoint and
                //update global for per minute
                try {
                    writeHBaseCheckpointXid();
                } catch (IOException e) {
                    logger.error("persistence xid pos error");
                    e.printStackTrace();
                }
            }
            //after persistence reinitialize the state
            eventList.clear();//the position is here???
            startTime = new Date().getTime();
            //monitor update and after monitor
            persistenceMonitor.endDealTime = new Date().getTime();
            persistenceMonitor.endTimeDate = new Date();
            persistenceMonitor.duringDealTime = persistenceMonitor.endDealTime - persistenceMonitor.startDealTime;
            if(persistenceMonitor.outEventNum > 0) {
                /*logger.info(":::::::::::::::::::::::::::::::::::::::::::::::::MONITOR : persistences data : count -> " +
                                persistenceMonitor.outEventNum +
                                ",size(event length) -> " +
                                persistenceMonitor.outSizeEvents +
                                ",start time -> " +
                                persistenceMonitor.startTimeDate +
                                ",end time -> " +
                                persistenceMonitor.endTimeDate +
                                ",during time -> " +
                                persistenceMonitor.duringDealTime
                );*/
                //whale monitor
                try {
//                    whaleMonitorProducer.send(0, String.valueOf(persistenceMonitor.outEventNum));
//                    whaleMonitorProducer.send(0, String.valueOf(persistenceMonitor.outSizeEvents));
//                    whaleMonitorProducer.send(0, String.valueOf(persistenceMonitor.startTimeDate));
//                    whaleMonitorProducer.send(0, String.valueOf(persistenceMonitor.endTimeDate));
//                    whaleMonitorProducer.send(0, String.valueOf(persistenceMonitor.duringDealTime));
                    String key = "tracker:" + new Date();
                    kafkaMonitorProducer.send(key, String.valueOf(persistenceMonitor.outEventNum));
                    kafkaMonitorProducer.send(key, String.valueOf(persistenceMonitor.outSizeEvents));
                    kafkaMonitorProducer.send(key, String.valueOf(persistenceMonitor.startTimeDate));
                    kafkaMonitorProducer.send(key, String.valueOf(persistenceMonitor.endTimeDate));
                    kafkaMonitorProducer.send(key, String.valueOf(persistenceMonitor.duringDealTime));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            //after monitor
            persistenceMonitor.clear();
        }

    }

    private void writeHBaseEvent() throws IOException{
        byte[] startPos = globalEventRowKey;
        List<Put> puts = new ArrayList<Put>();
        for(LogEvent event : eventList){
            CanalEntry.Entry entry = null;
            try {
                entry = eventParser.parse(event);
            } catch (Exception e){
                logger.error("parse to entry failed!!!");
                e.printStackTrace();
            }
            //globalize
            globalBinlogName = eventParser.getBinlogFileName();
            if(entry!=null) {
                Put put = new Put(startPos);
                put.add(hbaseOP.getFamily(), Bytes.toBytes(hbaseOP.eventBytesCol), entry.toByteArray());
                puts.add(put);
                //get next pos
                startPos = Bytes.toBytes(Bytes.toLong(startPos) + 1L);
                //update to global xid,
                // checkpoint pos record the xid event or row key ' s next pos not current pos
                if(isEndEvent(event)){
                    //globalize
                    globalXidEvent = event;
                    globalXidEventRowKey = startPos;
                }
            }
        }
        hbaseOP.putHBaseData(puts, hbaseOP.getEventBytesSchemaName());
        //globalize, checkpoint pos record the xid event or row key ' s next pos not current pos
        globalEventRowKey = startPos;
    }

    private boolean existXid(List<LogEvent> eventList){
        for(LogEvent event : eventList){
            if(isEndEvent(event)){
                return(true);
            }
        }
        return(false);
    }

    private boolean isEndEvent(LogEvent event){
        if((event.getHeader().getType()==LogEvent.XID_EVENT)
                ||(event.getHeader().getType()==LogEvent.QUERY_EVENT
                && !StringUtils.endsWithIgnoreCase(((QueryLogEvent) event).getQuery(), "BEGIN"))){
            return (true);
        }
        else    return(false);
    }

    private void writeHBaseCheckpointXid() throws IOException{
        Put put = new Put(Bytes.toBytes(hbaseOP.trackerRowKey));
        String xidValue = globalBinlogName + ":" + globalXidEvent.getLogPos();
        Long xidEventRowLong = Bytes.toLong(globalXidEventRowKey);
        String xidEventRowString = String.valueOf(xidEventRowLong);
        put.add(hbaseOP.getFamily(), Bytes.toBytes(hbaseOP.binlogXidCol), Bytes.toBytes(xidValue));
        put.add(hbaseOP.getFamily(), Bytes.toBytes(hbaseOP.eventXidCol), Bytes.toBytes(xidEventRowString));
        hbaseOP.putHBaseData(put, hbaseOP.getCheckpointSchemaName());
    }
}
