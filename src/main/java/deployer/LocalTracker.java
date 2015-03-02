package deployer;

import tracker.HandlerKafkaZkLocal;

/**
 * Created by hp on 15-3-2.
 */
public class LocalTracker {

    private static boolean running = true;

    public static void main(String[] args) throws Exception {
        final HandlerKafkaZkLocal handler = new HandlerKafkaZkLocal();
        handler.prepare("mysql-tracker-json");
        while(running) {
            handler.run();
        }
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    running = false;
                    handler.close("mysql-tracker-json");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

}
