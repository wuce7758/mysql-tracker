package com.jd.bdp.mysql.tracker;

import com.jd.bdp.magpie.Topology;

/**
 * Created by hp on 14-9-22.
 */
public class LogTracker {

    public static void main(String[] args) throws Exception {
        HandlerMagpieKafka handler = new HandlerMagpieKafka();
        Topology topology = new Topology(handler);
        topology.run();
    }

}
