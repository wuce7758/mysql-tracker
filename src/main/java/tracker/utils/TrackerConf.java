package tracker.utils;

import kafka.utils.KafkaConf;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import protocol.json.ConfigJson;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by hp on 14-12-12.
 */
public class TrackerConf {
    public Logger logger = LoggerFactory.getLogger(TrackerConf.class);

    //mysql conf
    public String username = "canal";
    public String password = "canal";
    public String address = "127.0.0.1";
    public int myPort = 3306;
    public long slaveId = 15879;
    //kafka conf
    public static String brokerList = "localhost:9092";//"12:9092,13.9092,14:9092"
    public static int kafkaPort = 9092;
    public static String zkKafka = "localhost:2181";
    public static String serializer = "kafka.serializer.DefaultEncoder";//default is byte[]
    public static String keySerializer = "kafka.serializer.StringEncoder";
    public static String partitioner = "kafka.producer.DefaultPartitioner";
    public static String acks = "-1";
    public static String topic = "test";//queue topic
    public static int partition = 0;
    public static String strSeeds = "127.0.0.1";//"172.17.36.53,172.17.36.54,172.17.36.55";
    public static List<String> brokerSeeds = new ArrayList<String>();//"12,13,14"
    //zk conf
    public String zkServers = "127.0.0.1:2181";//"48:2181,19:2181,50:2181"
    public int timeout = 100000;
    public String rootPath = "/checkpoint";
    public String persisPath = rootPath + "/persistence";
    public String minutePath = rootPath + "/minutes";
    //tracker conf
    public int batchsize = 10000;
    public int queuesize = 50000;
    public int sumBatch = 5 * batchsize;
    public int timeInterval = 1;
    public int reInterval = 3;
    public String filterRegex = ".*\\..*";
    public int minsec = 60;
    public int heartsec = 1 * 60;//10
    public int retrys = 10;//if we retry 100 connect or send failed too, we will reload the job //interval time
    public double mbUnit = 1024.0 * 1024.0;
    public String jobId = "mysql-tracker";
    public int spacesize = 8;//8 MB
    public int monitorsec = 60;//1 minute
    //phenix monitor
    public String phKaBrokerList = "localhost:9092";
    public int phKaPort = 9092;
    public String phKaZk = "localhost:2181";
    public String phKaSeria = "kafka.serializer.DefaultEncoder";
    public String phKaKeySeria = "kafka.serializer.StringEncoder";
    public String phKaParti = "kafka.producer.DefaultPartitioner";
    public String phKaAcks = "1";
    public String phKaTopic = "test1";
    public int phKaPartition = 0;
    //charset mysql tracker
    public Charset charset = Charset.forName("UTF-8");
    //filter same to parser
    public static Map<String, String> filterMap = new HashMap<String, String>();
    //position
    public String logfile = null;
    public long offset = -1;
    public long batchId = 0;
    public long inId = 0;
    public String CLASS_PREFIX = "classpath:";

    //constants
    private static String confPath = "tracker.properties";


    public void initConfLocal() {
        brokerSeeds.add("127.0.0.1");
    }

    public void initConfStatic() {
        username = "jd_data";
        password = "jd_data";
        address = "172.17.36.48";
        myPort = 3306;
        brokerList = "172.17.36.53:9092,172.17.36.54:9092,172.17.36.55:9092";
        strSeeds = "172.17.36.53,172.17.36.54,172.17.36.55";
        String ss[] = strSeeds.split(",");
        for(String s : ss) {
            brokerSeeds.add(s);
        }
        kafkaPort = 9092;
        zkKafka = "172.17.36.60/kafka";
        topic = "mysql_log";
        zkServers = "172.17.36.60:2181,172.17.36.61:2181,172.17.36.62:2181";
    }

    public void initConfFile() throws Exception {
        clear();
        String cnf = System.getProperty("tracker.conf", "classpath:tracker.properties");
        logger.info("load file : " + cnf);
        InputStream in = null;
        if(cnf.startsWith(CLASS_PREFIX)) {
            cnf = StringUtils.substringAfter(cnf, CLASS_PREFIX);
            in = TrackerConf.class.getClassLoader().getResourceAsStream(cnf);
        } else {
            in = new FileInputStream(cnf);
        }
        Properties pro = new Properties();
        pro.load(in);
        //load the parameter
        jobId = pro.getProperty("job.name");
        charset = Charset.forName(pro.getProperty("job.charset"));
        address = pro.getProperty("mysql.address");
        myPort = Integer.valueOf(pro.getProperty("mysql.port"));
        username = pro.getProperty("mysql.usr");
        password = pro.getProperty("mysql.psd");
        slaveId = Long.valueOf(pro.getProperty("mysql.slaveId"));
        String dataKafkaZk = pro.getProperty("kafka.data.zkserver") + pro.getProperty("kafka.data.zkroot");
        KafkaConf dataCnf = new KafkaConf();
        dataCnf.loadZk(dataKafkaZk);
        brokerList = dataCnf.brokerList;
        acks = pro.getProperty("kafka.acks");
        topic = pro.getProperty("kafka.data.topic.tracker");
        zkServers = pro.getProperty("zookeeper.servers");
        String monitorKafkaZk = pro.getProperty("kafka.monitor.zkserver") + pro.getProperty("kafka.monitor.zkroot");
        KafkaConf monitorCnf = new KafkaConf();
        monitorCnf.loadZk(monitorKafkaZk);
        phKaBrokerList = monitorCnf.brokerList;
        phKaTopic = pro.getProperty("kafka.monitor.topic");
        in.close();
    }

    public void initConfJSON() {
        ConfigJson jcnf = new ConfigJson(jobId, "magpie.address");
        JSONObject root = jcnf.getJson();
        //parser the json
        if(root != null) {
            JSONObject data = root.getJSONObject("info").getJSONObject("content");
            username = data.getString("username");
            password = data.getString("password");
            address = data.getString("address");
            myPort = Integer.valueOf(data.getString("myPort"));
            slaveId = Long.valueOf(data.getString("slaveId"));
            brokerList = data.getString("brokerList");
            kafkaPort = Integer.valueOf(data.getString("kafkaPort"));
            zkKafka = data.getString("zkKafka");
            topic = data.getString("topic");
            strSeeds = data.getString("strSeeds");
            zkServers = data.getString("zkServers");
            filterRegex = data.getString("filterRegex");
        }
    }

    public void initConfOnlineJSON() throws Exception {
        clear();
        ConfigJson jcnf = new ConfigJson(jobId, "relase.address");
        JSONObject root = jcnf.getJson();
        //parse the json
        if(root != null) {
            int _code = root.getInt("_code");
            if(_code != 0) {
                String errMsg = root.getString("errorMessage");
                throw new Exception(errMsg);
            }
            JSONObject data = root.getJSONObject("data");
            //mysql simple load
            username = data.getString("source_user");
            password = data.getString("source_password");
            address = data.getString("source_host");
            myPort = Integer.valueOf(data.getString("source_port"));
            slaveId = Long.valueOf(data.getString("slaveId"));
            charset = Charset.forName(data.getString("source_charset"));
            //get kafka parameter from zk
            String dataKafkaZk = data.getString("kafka_zkserver") + data.getString("kafka_zkroot");
            KafkaConf dataCnf = new KafkaConf();
            dataCnf.loadZk(dataKafkaZk);
            brokerList = dataCnf.brokerList;
            //kafka simple load json
            acks = data.getString("kafka_acks");
            topic = data.getString("tracker_log_topic");
            //load own zk
            zkServers = data.getString("offset_zkserver");
            //get kafka monitor parameter from zk
            String monitorKafkaZk = data.getString("monitor_server") + data.getString("monitor_zkroot");
            KafkaConf monitorCnf = new KafkaConf();
            monitorCnf.loadZk(monitorKafkaZk);
            phKaBrokerList = monitorCnf.brokerList;
            //kafka simple loadjson
            phKaTopic = data.getString("monitor_topic");
            //jobId
            jobId = data.getString("job_id");
            //filter load
            if(data.containsKey("db_tab_meta")) {
                JSONArray jf = data.getJSONArray("db_tab_meta");
                for (int i = 0; i <= jf.size() - 1; i++) {
                    JSONObject jdata = jf.getJSONObject(i);
                    String dbname = jdata.getString("dbname");
                    String tbname = jdata.getString("tablename");
                    String key = dbname + "." + tbname;
                    String value = tbname;
                    filterMap.put(key, value);
                }
            }
            //load position
            if(data.containsKey("position-logfile")) {
                logfile = data.getString("position-logfile");
            }
            if(data.containsKey("position-offset")) {
                offset = Long.valueOf(data.getString("position-offset"));
            }
            if(data.containsKey("position-bid")) {
                batchId = Long.valueOf(data.getString("position-bid"));
            }
            if(data.containsKey("position-iid")) {
                inId = Long.valueOf(data.getString("position-iid"));
            }
        }
    }

    //clear the conf info
    public void clear() {
        brokerSeeds.clear();
        filterMap.clear();
    }
}
