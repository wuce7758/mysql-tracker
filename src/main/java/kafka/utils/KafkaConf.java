package kafka.utils;

import net.sf.json.JSONObject;
import zk.client.ZkExecutor;
import zk.utils.ZkConf;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hp on 14-12-12.
 */
public class KafkaConf {

    public static String brokerList = "localhost:9092";//"12:9092,13.9092,14:9092"
    public static int port = 9092;
    public static String zk = "localhost:2181";
    public static String serializer = "kafka.serializer.DefaultEncoder";//default is byte[]
    public static String keySerializer = "kafka.serializer.StringEncoder";//default is message's byte[]
    public static String partitioner = "kafka.producer.DefaultPartitioner";
    public static String acks = "1";
    public static String sendBufferSize = String.valueOf(1024 * 1024);//1MB
    public static String topic;//queue topic
    public static int partition = 0;
    public static List<String> topics = new ArrayList<String>();//distribute the multiple topic
    public static List<String> brokerSeeds = new ArrayList<String>();//"12,13,14"
    public static List<Integer> portList = new ArrayList<Integer>();//9092 9093 9094
    public static int readBufferSize = 1 * 1024 * 1024;//1 MB
    public static String clientName = "cc456687IUGHG";

    //load the zkPos to find the bokerList and port zkPos : 172.17.36.60:2181/kafka
    public static void loadZk(String zkPos) throws Exception{
        if(zkPos == null) throw new Exception("zk path is null");
        String[] ss = zkPos.split("/");
        String zkServer = "";
        String zkPath = "";
        for(int i = 0; i<= ss.length - 1; i++) {
            if(i == 0) {
                zkServer = ss[i];
            } else {
                zkPath += ("/" + ss[i]);
            }
        }
        if(ss.length == 1) {
            zkPath = "/";
        }
        zkPath += ("/brokers/ids");
        ZkConf zcnf = new ZkConf();
        zcnf.zkServers = zkServer;
        ZkExecutor zkexe = new ZkExecutor(zcnf);
        List<String> ids = zkexe.getChildren(zkPath);
        brokerList = "";
        brokerSeeds.clear();
        portList.clear();
        for(String brokerNode : ids) {
            String zkNodeJson = zkexe.get(zkPath + "/" + brokerNode);
            if(zkNodeJson == null) continue;
            JSONObject jo = JSONObject.fromObject(zkNodeJson);
            String host = jo.getString("host");
            int port = jo.getInt("port");
            brokerSeeds.add(host);
            portList.add(port);
            brokerList += (host + ":" + port);
        }
    }

}
