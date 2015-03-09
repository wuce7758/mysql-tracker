package deployer;

import tracker.SimpleMysqlTracker;

/**
 * Created by hp on 15-3-9.
 */
public class SimpleLocalTracker {
    public static void main(String[] args) throws Exception {
        SimpleMysqlTracker tracker = new SimpleMysqlTracker();
        tracker.start();
    }
}
