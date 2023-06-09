package tests;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import ros.Publisher;
import ros.RosBridge;
import ros.RosListenDelegate;
import ros.SubscriptionRequestMsg;
import ros.msgs.std_msgs.PrimitiveMsg;
import ros.tools.MessageUnpacker;

import java.util.concurrent.TimeUnit;

/**
 * Example of connecting to rosbridge with publish/subscribe messages. Takes one argument:
 * the rosbridge websocket URI; for example: ws://localhost:9090.
 *
 * @author James MacGlashan.
 */
public class RosTest {
    private static final String DEFAULT_ROSBRIDGE_WEBSOCKET_URI = "ws://localhost:9090";

    @Test
    public void test() {
        try {
            final RosBridge bridge = new RosBridge(DEFAULT_ROSBRIDGE_WEBSOCKET_URI);

            bridge.connect();


            {
                final boolean waitForConnection = bridge.waitForConnection(30, TimeUnit.SECONDS);
                Assume.assumeTrue("Could not connect in time, perhaps rosbridge is not online", waitForConnection);
            }
            {
                final boolean isConnected = bridge.isConnected();
                Assert.assertTrue("Is not connected", isConnected);
            }

            bridge.subscribe(SubscriptionRequestMsg.generate("/ros_to_java")
                            .setType("std_msgs/String")
                            .setThrottleRate(1)
                            .setQueueLength(1),
                    new RosListenDelegate() {

                        public final void receive(JsonNode data, String stringRep) {
                            MessageUnpacker<PrimitiveMsg<String>> unpacker = new MessageUnpacker<PrimitiveMsg<String>>(PrimitiveMsg.class);
                            PrimitiveMsg<String> msg = unpacker.unpackRosMessage(data);
                            System.out.println(msg.data);
                        }
                    }
            );

            final Publisher pub = new Publisher("/java_to_ros", "std_msgs/String", bridge);

            for (int i = 0; i < 100; i++) {
                pub.publish(new PrimitiveMsg<String>("hello from java " + i));
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (final Exception exception) {
            Assert.fail(ExceptionUtils.getStackTrace(exception));
        }
    }

    public static void main(String[] args) {

        if (args.length != 1) {
            System.out.println("Need the rosbridge websocket URI provided as argument. For example:\n\tws://localhost:9090");
            System.exit(0);
        }

        final RosBridge bridge = new RosBridge(args[0]);
        bridge.connect();
        bridge.waitForConnection(30, TimeUnit.SECONDS);

        bridge.subscribe(SubscriptionRequestMsg.generate("/ros_to_java")
                        .setType("std_msgs/String")
                        .setThrottleRate(1)
                        .setQueueLength(1),
                new RosListenDelegate() {

                    public final void receive(JsonNode data, String stringRep) {
                        MessageUnpacker<PrimitiveMsg<String>> unpacker = new MessageUnpacker<PrimitiveMsg<String>>(PrimitiveMsg.class);
                        PrimitiveMsg<String> msg = unpacker.unpackRosMessage(data);
                        System.out.println(msg.data);
                    }
                }
        );

        final Publisher pub = new Publisher("/java_to_ros", "std_msgs/String", bridge);

        for (int i = 0; i < 100; i++) {
            pub.publish(new PrimitiveMsg<String>("hello from java " + i));
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
