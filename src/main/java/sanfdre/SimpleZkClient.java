package sanfdre;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;

/**
 * @Author: lilinglin
 * @Description:
 * @Date: Created in 2018/10/26
 */
public class SimpleZkClient {
    private static String connectString = "192.168.138.101:2181,192.168.138.102:2181,192.168.138.103:2181";
    public static void main(String[] args) throws Exception {
        ZooKeeper zooKeeper = new ZooKeeper(connectString,2000, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                String path = watchedEvent.getPath();
                Event.EventType type = watchedEvent.getType();
            }
        });
        List<String> childrens = zooKeeper.getChildren("/",false,null);
        for (String children : childrens){
            System.out.println(children);
        }
    }
}
