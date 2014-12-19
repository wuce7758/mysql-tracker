package tracker;

import com.jd.bdp.magpie.MagpieExecutor;
import filter.FilterMatcher;
import kafka.driver.producer.KafkaSender;
import kafka.utils.KafkaConf;
import monitor.TrackerMonitor;
import mysql.dbsync.DirectLogFetcherChannel;
import mysql.dbsync.LogContext;
import mysql.dbsync.LogDecoder;
import mysql.dbsync.LogEvent;
import mysql.dbsync.event.QueryLogEvent;
import mysql.driver.MysqlConnector;
import mysql.driver.MysqlQueryExecutor;
import mysql.driver.MysqlUpdateExecutor;
import mysql.driver.packets.HeaderPacket;
import mysql.driver.packets.client.BinlogDumpCommandPacket;
import mysql.driver.packets.server.ResultSetPacket;
import mysql.driver.utils.PacketManager;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import protocol.protobuf.CanalEntry;
import tracker.common.TableMetaCache;
import tracker.parser.LogEventConvert;
import tracker.position.EntryPosition;
import tracker.utils.TrackerConf;
import zk.client.ZkExecutor;
import zk.utils.ZkConf;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by hp on 14-12-12.
 */
public class HandlerMagpieKafka implements MagpieExecutor {
    //logger
    private Logger logger = LoggerFactory.getLogger(HandlerMagpieKafka.class);
    //global config
    private TrackerConf config = new TrackerConf();
    //mysql interface
    private MysqlConnector logConnector;
    private MysqlConnector tableConnector;
    private MysqlConnector realConnector;
    private MysqlQueryExecutor queryExecutor;
    private MysqlUpdateExecutor updateExecutor;
    private MysqlQueryExecutor realQuery;
    //mysql table meta cache
    private TableMetaCache tableMetaCache;
    //mysql log event convert and filter
    private LogEventConvert eventConvert;
    //job id
    private String jobId;
    //kafka
    private KafkaSender msgSender;
    //zk
    private ZkExecutor zkExecutor;
    //blocking queue
    private BlockingQueue<LogEvent> eventQueue;
    //batch id and in batch id
    private long batchId = 0;
    private long inBatchId = 0;
    //thread communicate
    private int globalFetchThread = 0;
    //global var
    private LogEvent globalXidEvent = null;
    private String globalBinlogName = "null";
    private long globalXidBatchId = -1;
    private long globalXidInBatchId = -1;
    //filter
    private FilterMatcher fm;
    //global start time
    private long startTime;
    //thread
    Fetcher fetcher;
    Timer timer;
    Minuter minter;
    //monitor
    private TrackerMonitor monitor;
    //global var
    private List<CanalEntry.Entry> entryList;//filtered
    private LogEvent lastEvent = null;//get the eventList's last xid event
    private String binlog = null;


    //delay time
    private void delay(int sec) {
        try {
            Thread.sleep(sec * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    //init global config
    private void init() throws Exception {
        //log
        logger.info("initializing......");
        //load json to global config
//        MagpieConfigJson jcnf = new MagpieConfigJson(jobId);
//        JSONObject jRoot = jcnf.getJson();
//        if(jRoot != null) {
//            JSONObject jcontent = jRoot.getJSONObject("info").getJSONObject("content");
//            config.username = jcontent.getString("Username");
//            config.password = jcontent.getString("Password");
//        }
        //init test envrionment
        config.testInit();
        //generate the driver, interface etc.
        logConnector = new MysqlConnector(new InetSocketAddress(config.address, config.myPort),
                config.username,
                config.password);
        tableConnector = new MysqlConnector(new InetSocketAddress(config.address, config.myPort),
                config.username,
                config.password);
        realConnector = new MysqlConnector(new InetSocketAddress(config.address, config.myPort),
                config.username,
                config.password);
        boolean mysqlExists = false;
        while (!mysqlExists) {
            try {
                logConnector.connect();
                tableConnector.connect();
                realConnector.connect();
                mysqlExists = true;
            } catch (IOException e) {
                logger.error("connect mysql failed ... retry to connect it...");
                e.printStackTrace();
                delay(5);
            }
        }
        queryExecutor = new MysqlQueryExecutor(logConnector);
        updateExecutor = new MysqlUpdateExecutor(logConnector);
        realQuery = new MysqlQueryExecutor(realConnector);
        //table meta cache
        tableMetaCache = new TableMetaCache(tableConnector);
        //queue
        eventQueue = new LinkedBlockingQueue<LogEvent>(config.queuesize);
        //kafka
        KafkaConf kcnf = new KafkaConf();
        kcnf.brokerList = config.brokerList;
        kcnf.port = config.kafkaPort;
        kcnf.topic = config.topic;
        msgSender = new KafkaSender(kcnf);
        msgSender.connect();
        //zk
        ZkConf zcnf = new ZkConf();
        zcnf.zkServers = config.zkServers;
        zkExecutor = new ZkExecutor(zcnf);
        zkExecutor.connect();
        initZk();
        //filter
        fm = new FilterMatcher(config.filterRegex);
        //event convert
        eventConvert = new LogEventConvert();
        eventConvert.setTableMetaCache(tableMetaCache);
        eventConvert.filter = fm;
        //start time configuration
        startTime = System.currentTimeMillis();
        //global fetch thread
        globalFetchThread = 0;
        //thread config
        fetcher = new Fetcher();
        timer = new Timer();
        minter = new Minuter();
        //monitor
        monitor = new TrackerMonitor();
        //batch id
        batchId = 0;
        inBatchId = 0;
        //global var
        entryList = new ArrayList<CanalEntry.Entry>();
    }

    private void initZk() throws Exception {
        if(!zkExecutor.exists(config.rootPath)) {
            zkExecutor.create(config.rootPath,"");
        }
        if(!zkExecutor.exists(config.persisPath)) {
            zkExecutor.create(config.persisPath,"");
        }
        if(!zkExecutor.exists(config.minutePath)) {
            zkExecutor.create(config.minutePath,"");
        }
    }

    private EntryPosition findPosFromMysqlNow() {
        EntryPosition returnPos = null;
        try {
            ResultSetPacket resultSetPacket = queryExecutor.query("show master status");
            List<String> fields = resultSetPacket.getFieldValues();
            if(CollectionUtils.isEmpty(fields)) {
                throw new Exception("show master status failed");
            }
            returnPos = new EntryPosition(fields.get(0), Long.valueOf(fields.get(1)));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return returnPos;
    }

    private EntryPosition findPosFromZk() {
        logger.info("finding position......");
        EntryPosition returnPos = null;
        try {
            String getStr = zkExecutor.get(config.persisPath);
            if(getStr == null || getStr.equals("")) {
                logger.info("find mysql show master status......");
                returnPos = findPosFromMysqlNow();
                batchId = 0;
                inBatchId = 0;
                logger.info("start position :" + returnPos.getBinlogPosFileName()+":"+returnPos.getPosition()+
                ":"+batchId+
                ":"+inBatchId);
                return returnPos;
            }
            String[] ss = getStr.split(":");
            if(ss.length != 4) {
                zkExecutor.delete(config.persisPath);
                logger.error("zk position format is error...");
                return null;
            }
            logger.info("find zk position......");
            returnPos = new EntryPosition(ss[0], Long.valueOf(ss[1]));
            batchId = Long.valueOf(ss[2]);
            inBatchId = Long.valueOf(ss[3]);
            logger.info("start position :" + returnPos.getBinlogPosFileName()+":"+returnPos.getPosition()+
                    ":"+batchId+
                    ":"+inBatchId);
        } catch (Exception e) {
            logger.error("zk client error : " + e.getMessage());
            e.printStackTrace();
        }
        return returnPos;
    }

    public void prepare(String id) throws Exception {
        jobId = id;
        init();
        //start thread
        fetcher.start();
        timer.schedule(minter, 1000, config.minsec * 1000);
        //log
        logger.info("start the tracker successfully......");
    }

    class Fetcher extends Thread {
        private DirectLogFetcherChannel fetcher;
        private LogDecoder decoder;
        private LogContext context;
        private Logger logger = LoggerFactory.getLogger(Fetcher.class);
        private LogEvent event;
        private TrackerMonitor monitor = new TrackerMonitor();

        public boolean iskilled = false;

        public void run() {
            try {
                init();
                int counter = 0;
                while (fetcher.fetch()) {
                    if (counter == 0) monitor.fetchStart = System.currentTimeMillis();
                    event = decoder.decode(fetcher, context);
                    if(event == null) {
                        logger.warn("fetched event is null...");
                        continue;
                    }
                    counter++;
                    monitor.batchSize += event.getEventLen();
                    //add the event to the queue
                    eventQueue.put(event);
                    if(counter % 10000 == 0) {
                        monitor.fetchEnd = System.currentTimeMillis();
                        logger.info("===================================> fetch thread : ");
                        logger.info("---> fetch during time : " + (monitor.fetchEnd - monitor.fetchStart));
                        logger.info("---> fetch number : " + counter);
                        logger.info("---> fetch sum size : " + monitor.batchSize);
                        monitor.clear();
                        counter = 0;
                    }
                    if(iskilled) break;
                }
            } catch (Exception e) {
                logger.error("fetch thread error : " + e.getMessage());
                e.printStackTrace();
                String errMsg = e.getMessage();
                if(errMsg.contains("errno = 1236")) {
                    try {
                        zkExecutor.delete(config.persisPath);
                    } catch (Exception e1) {
                        logger.error(e1.getMessage());
                        e1.printStackTrace();
                    }
                    globalFetchThread = 1;
                }
                if(errMsg.contains("zk position is error")) {
                    globalFetchThread = 1;
                }
            }
        }

        private void init() throws Exception {
            //find start position
            EntryPosition startPos = findPosFromZk();
            if(startPos == null) throw new Exception("zk position is error...");
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
            binDmpPacket.binlogFileName = startPos.getJournalName();
            binDmpPacket.binlogPosition = startPos.getPosition();
            binDmpPacket.slaveServerId = config.slaveId;
            byte[] dmpBody = binDmpPacket.toBytes();
            HeaderPacket dmpHeader = new HeaderPacket();
            dmpHeader.setPacketBodyLength(dmpBody.length);
            dmpHeader.setPacketSequenceNumber((byte) 0x00);
            PacketManager.write(logConnector.getChannel(), new ByteBuffer[]{ByteBuffer.wrap(dmpHeader.toBytes()), ByteBuffer.wrap(dmpBody)});
            //initialize the mysql.dbsync to fetch the binlog data
            fetcher = new DirectLogFetcherChannel(logConnector.getReceiveBufferSize());
            fetcher.start(logConnector.getChannel());
            decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
            context = new LogContext();
        }
    }

    class Minuter extends TimerTask {

        private Logger logger = LoggerFactory.getLogger(Minuter.class);

        @Override
        public void run(){
            try {
                Calendar cal = Calendar.getInstance();
                DateFormat sdf = new SimpleDateFormat("HH:mm");
                DateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd");
                String time = sdf.format(cal.getTime());
                String date = sdfDate.format(cal.getTime());
                String xidValue = null;
                long pos = -1;
                if(globalXidEvent != null) {
                    pos = globalXidEvent.getLogPos();
                    xidValue = globalBinlogName + ":" + globalXidEvent.getLogPos() + ":" + globalXidBatchId + ":" + globalXidInBatchId;
                } else {
                    pos = -1;
                    xidValue = globalBinlogName + ":" + "-1" + ":" + globalXidBatchId + ":" + globalXidInBatchId;
                }
                if(!zkExecutor.exists(config.minutePath+"/"+date)) {
                    zkExecutor.create(config.minutePath+"/"+date,date);
                }
                if(!zkExecutor.exists(config.minutePath+"/"+date+"/"+time)) {
                    zkExecutor.create(config.minutePath + "/" + date + "/" + time, xidValue);
                } else  {
                    zkExecutor.set(config.minutePath + "/" + date + "/" + time, xidValue);
                }
                logger.info("===================================> per minute thread :");
                logger.info("---> binlog file is " + globalBinlogName +
                        ",position is :" + pos + "; batch id is :" + globalXidBatchId +
                        ",in batch id is :" + globalXidInBatchId);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void run() throws Exception {
        //check fetch thread status
        if(globalFetchThread == 1) {
            globalFetchThread = 0;
            reload(jobId);
            return;
        }
        //take the data from the queue
        while (!eventQueue.isEmpty()) {
            LogEvent event = eventQueue.take();
            if(event == null) continue;
            if(isEndEvent(event)) lastEvent = event;
            CanalEntry.Entry entry = eventConvert.parse(event);
            if(entry != null) {
                eventConvert.setBatchId(batchId);
                eventConvert.setInId(inBatchId);
                inBatchId++;//batchId.inId almost point next event's position
                if(isEndEvent(event)) {
                    inBatchId = 0;
                    batchId++;
                }
                entryList.add(CanalEntry.Entry.newBuilder(entry).build());//build or new object
            }
            if(event == lastEvent) {
                binlog = eventConvert.getBinlogFileName();
                globalBinlogName = binlog;
                globalXidEvent = event;
                globalXidBatchId = batchId;
                globalXidInBatchId = inBatchId;
            }
            if(entryList.size() >= config.batchsize) break;
        }
        // serialize the list -> filter -> batch for it -> send the batched bytes to the kafka; persistence the batched list???
        // or no batched list???
        // I got it : mysqlbinlog:pos could be no filtered event but batchId and inBatchId must be filtered event
        //     so the mysqlbinlog:pos <--> batchId:inBatchId Do not must be same event to same event
        // mysqlbinlog:pos <- no filter list's xid  batchid:inBatchId <- filter list's last event
        //entryList data to kafka , per time must confirm the position
        if(entryList.size() > config.batchsize || (System.currentTimeMillis() - startTime) > config.timeInterval * 1000 ) {
            monitor.persisNum = entryList.size();
            persisteData(entryList);//send the data list to kafka
            confirmPos(lastEvent,binlog);//send the mysql pos batchid inbatchId to zk
            startTime = System.currentTimeMillis();
            entryList.clear();
        }
        if(monitor.persisNum > 0) {
            logger.info("===================================> persistence thread:");
            logger.info("---> persistence deal during time:" + (monitor.persistenceEnd - monitor.persistenceStart));
            logger.info("---> write kafka during time:" + (monitor.hbaseWriteEnd - monitor.hbaseWriteStart));
            logger.info("---> the number of entry list: " + monitor.persisNum);
            logger.info("---> entry list to bytes sum size is " + monitor.batchSize);
            if(lastEvent != null)
                logger.info("---> position info:"+" binlog file is " + globalBinlogName +
                    ",position is :" + lastEvent.getLogPos() + "; batch id is :" + globalXidBatchId +
                    ",in batch id is :" + globalXidInBatchId);
            monitor.clear();
        }
    }

    private void persisteData(List<CanalEntry.Entry> entries) {
        monitor.persistenceStart = System.currentTimeMillis();
        List<byte[]> bytesList = new ArrayList<byte[]>();
        for(CanalEntry.Entry entry : entries) {
            byte[] value = entry.toByteArray();
            bytesList.add(value);
            monitor.batchSize += value.length;
        }
        monitor.persistenceEnd = System.currentTimeMillis();
        monitor.hbaseWriteStart = System.currentTimeMillis();
        if(bytesList.size() > 0) msgSender.send(bytesList);
        monitor.hbaseWriteEnd = System.currentTimeMillis();

    }

    private void confirmPos(LogEvent last, String bin) throws Exception {
        if(last != null) {
            String pos = bin + ":" + last.getLogPos() + ":" + batchId + ":" + inBatchId;
            zkExecutor.set(config.persisPath, pos);
        }
    }

    private boolean isEndEvent(LogEvent event){
        if((event.getHeader().getType()==LogEvent.XID_EVENT)
                ||(event.getHeader().getType()==LogEvent.QUERY_EVENT
                && !StringUtils.endsWithIgnoreCase(((QueryLogEvent) event).getQuery(), "BEGIN"))){
            return (true);
        }
        else    return(false);
    }

    public void pause(String id) throws Exception {

    }

    public void reload(String id) throws Exception {
        close(jobId);
        prepare(jobId);
    }

    public void close(String id) throws Exception {
        fetcher.iskilled = true;//stop the fetcher thread
        minter.cancel();//stop the per minute record
        timer.cancel();
        logConnector.disconnect();
        realConnector.disconnect();
        tableConnector.disconnect();
        msgSender.close();
        zkExecutor.close();
    }
}
