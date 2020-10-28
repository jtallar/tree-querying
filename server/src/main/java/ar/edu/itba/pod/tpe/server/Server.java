package ar.edu.itba.pod.tpe.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hazelcast.core.Hazelcast;

public class Server {
    private static Logger logger = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args) {
        logger.info("tpe2-g6 Server Starting ...");
        Hazelcast.newHazelcastInstance();
        logger.info("Node started");
    }
}
