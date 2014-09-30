package com.jd.bdp.mysql.tracker;

import com.jd.bdp.magpie.Topology;

/**
 * Created by hp on 14-9-22.
 */
public class LogTracker {

    public static void main(String[] args) throws Exception {
        //Handler handler = new Handler("canal","canal","192.168.213.41",3306,Long.valueOf(7777));
        Handler1 handler = new Handler1("canal","canal","127.0.0.1",3306,Long.valueOf(2234),"localhost:9000/hbase");
        Topology topology = new Topology(handler);
        topology.run();
    }

}
