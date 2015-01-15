package tracker.utils;

import kafka.utils.KafkaConf;
import net.sf.json.JSONObject;
import protocol.json.ConfigJson;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hp on 14-12-12.
 */
public class TrackerConf {
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
    public int retrys = 100;//if we retry 100 connect or send failed too, we will reload the job //interval time
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
        ConfigJson jcnf = new ConfigJson(jobId, "offline.address");
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
        }
    }

    //clear the conf info
    public void clear() {
        brokerSeeds.clear();
    }
}
