package ros;


import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

import java.io.Closeable;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A socket for connecting to ros bridge that accepts subscribe and publish commands.
 * Subscribing to a topic using the {@link #subscribe(SubscriptionRequestMsg, RosListenDelegate)}.
 * The input {@link SubscriptionRequestMsg} allows you to iteratively build all the optional fields
 * you can set to detail the subscription, such as whether the messages should be fragmented in size,
 * the queue length, throttle rate, etc. If data is pushed quickly, it is recommended that you
 * set the throttle rate and queue length to 1 or you may observe increasing latency in the messages.
 * Png compression is currently not supported. If the message type
 * is not set, and the topic either does not exist or you have never subscribed to that topic previously,
 * Rosbridge may fail to subscribe. There are also additional methods for subscribing that take the parameters
 * of a subscription as arguments to the method.
 * <p>
 * Publishing is also supported with the {@link #publish(String, String, Object)} method, but you should
 * consider using the {@link ros.Publisher} class wrapper for streamlining publishing.
 * <p>
 * To create and connect to rosbridge, you can either instantiate with the default constructor
 * and then call {@link #connect(String)} or use the static method {@link #createConnection(String)} which
 * creates a RosBridge instance and then connects.
 * An example URI to provide as a parameter is: ws://localhost:9090, where 9090 is the default Rosbridge server port.
 * <p>
 * If you need to handle messages with larger sizes, you should subclass RosBridge and annotate the class
 * with {@link WebSocket} with the parameter maxTextMessageSize set to the desired buffer size. For example:
 * <p>
 * <code>
 * {@literal @}WebSocket(maxTextMessageSize = 500 * 1024)  public class BigRosBridge extends RosBridge{  }
 * </code>
 * <p>
 * Note that the subclass does not need to override any methods; subclassing is performed purely to set the
 * buffer size in the annotation value. Then you can instantiate BigRosBridge and call its inherited connect method.
 *
 * @author James MacGlashan.
 */
@WebSocket
public class RosBridge {
    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    protected final CountDownLatch closeLatch;
    protected Session session;

    protected Map<String, RosBridgeSubscriber> listeners = new ConcurrentHashMap<String, RosBridge.RosBridgeSubscriber>();
    protected Set<String> publishedTopics = new CopyOnWriteArraySet<>();

    protected Map<String, FragmentManager> fragmentManagers = new ConcurrentHashMap<>();

    protected boolean hasConnected = false;

    protected boolean logMessagesAsReceived = false;


    /**
     * Creates a default RosBridge and connects it to the ROS Bridge websocket server located at rosBridgeURI.
     * Note that it is recommend that you call the {@link #waitForConnection()} method
     * before publishing or subscribing.
     *
     * @param rosBridgeURI the URI to the ROS Bridge websocket server. Note that ROS Bridge by default uses port 9090. An example URI is: ws://localhost:9090
     * @return the ROS Bridge socket that is connected to the indicated server.
     */
    public static RosBridge createConnection(String rosBridgeURI) {

        final RosBridge socket = new RosBridge();
        socket.connect(rosBridgeURI);
        return socket;

    }


    /**
     * Connects to the Rosbridge host at the provided URI. Does not wait for connection.
     *
     * @param rosBridgeURI the URI to the ROS Bridge websocket server. Note that ROS Bridge by default uses port 9090. An example URI is: ws://localhost:9090
     */
    public void connect(final String rosBridgeURI) {
        this.connect(rosBridgeURI, false);
    }

    /**
     * Connects to the Rosbridge host at the provided URI.
     *
     * @param rosBridgeURI      the URI to the ROS Bridge websocket server. Note that ROS Bridge by default uses port 9090. An example URI is: ws://localhost:9090
     * @param waitForConnection if true, then this method will block until the connection is established. If false, then return immediately.
     */
    public void connect(String rosBridgeURI, boolean waitForConnection) {
        final WebSocketClient client = new WebSocketClient();
        try {
            client.start();
            final URI echoUri = new URI(rosBridgeURI);
            final ClientUpgradeRequest request = new ClientUpgradeRequest();
            client.connect(this, echoUri, request);

            if (LOGGER.isTraceEnabled()) {
                final String msg = String.format("Connecting to : %s%n", echoUri);
                LOGGER.trace(msg);
            }

            if (waitForConnection) {
                this.waitForConnection();
            }

        } catch (final Throwable throwable) {
            LOGGER.error(ExceptionUtils.getStackTrace(throwable));
            throw new RuntimeException(throwable);
        }

    }

    public RosBridge() {
        this.closeLatch = new CountDownLatch(1);
    }


    /**
     * Blocks execution until a connection to the ros bridge server is established.
     */
    public void waitForConnection() {

        if (this.hasConnected) {
            return; //done
        }

        synchronized (this) {
            while (!this.hasConnected) {
                try {
                    this.wait();
                } catch (final InterruptedException interruptedException) {
                    LOGGER.error(ExceptionUtils.getStackTrace(interruptedException));
                }
            }
        }

    }


    /**
     * Indicates whether the connection has been made
     *
     * @return a boolean indicating whether the connection has been made
     */
    public boolean hasConnected() {
        return this.hasConnected;
    }


    /**
     * Returns whether ROSBridge will log all ROSBridge messages as they are received to the command line.
     *
     * @return if true, then ROSBridge will log all ROSBridge messages as they are received to the command line. Otherwise is silent.
     */
    public boolean logMessagesAsReceived() {
        return logMessagesAsReceived;
    }

    /**
     * Sets whether ROSBridge should log all ROSBridge messages as they are received to the command.
     *
     * @param logMessagesAsReceived if true, then ROSBridge will log all ROSBridge messages as they are received to the command line. Otherwise is silent.
     */
    public void setLogMessagesAsReceived(boolean logMessagesAsReceived) {
        this.logMessagesAsReceived = logMessagesAsReceived;
    }


    /**
     * Use this method to close the connection. Will automatically unsubscribe and unadvertise from all topics first.
     * Call the {@link #awaitClose(int, TimeUnit)} method if you want to block a thread until closing has finished up.
     */
    public void closeConnection() {
        this.unsubsribeUnAdvertiseAll();
        this.session.close();
    }

    /**
     * Use this to to wait for a connection to close, or a maximum amount of time.
     *
     * @param duration the time in some units until closing.
     * @param unit     the unit of time in which duration is measured.
     * @return the result of the {@link java.util.concurrent.CountDownLatch#await()} method. true if closing happened;
     * false if time ran out.
     * @throws InterruptedException
     */
    public boolean awaitClose(int duration, TimeUnit unit) throws InterruptedException {
        return this.closeLatch.await(duration, unit);
    }

    @OnWebSocketClose
    public void onClose(int statusCode, String reason) {

        if (LOGGER.isTraceEnabled()) {
            final String msg = String.format("Connection closed: %d - %s%n", statusCode, reason);
            LOGGER.trace(msg);
        }

        this.session = null;
        this.closeLatch.countDown();
    }

    @OnWebSocketConnect
    public void onConnect(Session session) {
        if (LOGGER.isTraceEnabled()) {
            final String msg = String.format("Got connect for ros: %s%n", session);
            LOGGER.trace(msg);
        }
        this.session = session;
        this.hasConnected = true;
        synchronized (this) {
            this.notifyAll();
        }

    }

    @OnWebSocketMessage
    public void onMessage(String msg) {

        if (this.logMessagesAsReceived && LOGGER.isDebugEnabled()) {

            LOGGER.debug(msg);
        }

        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = null;
        try {
            node = mapper.readTree(msg);
            if (node.has("op")) {
                String op = node.get("op").asText();
                if (op.equals("publish")) {
                    String topic = node.get("topic").asText();
                    RosBridgeSubscriber subscriber = this.listeners.get(topic);
                    if (subscriber != null) {
                        subscriber.receive(node, msg);
                    }
                } else if (op.equals("fragment")) {
                    this.processFragment(node);
                }
            }
        } catch (final IOException ioException) {
            final String errorMsg = "Could not parse ROSBridge web socket message into JSON data";
            LOGGER.error(errorMsg + " " + ExceptionUtils.getStackTrace(ioException));
            throw new RuntimeException(ioException);
        }


    }


    /**
     * Subscribes to a ros topic. New publish results will be reported to the provided delegate.
     * If message type is null, then the type will be inferred. When type is null, if a topic
     * does not already exist, subscribe will fail.
     *
     * @param topic    the to subscribe to
     * @param type     the message type of the topic. Pass null for type inference.
     * @param delegate the delegate that receives updates to the topic
     */
    public void subscribe(String topic, String type, RosListenDelegate delegate) {
        this.subscribe(SubscriptionRequestMsg.generate(topic).setType(type), delegate);
    }


    /**
     * Subscribes to a ros topic. New publish results will be reported to the provided delegate.
     * If message type is null, then the type will be inferred. When type is null, if a topic
     * does not already exist, subscribe will fail.
     *
     * @param topic        the to subscribe to
     * @param type         the message type of the topic. Pass null for type inference.
     * @param delegate     the delegate that receives updates to the topic
     * @param throttleRate the minimum amount of time (in ms) that must elapse between messages being sent from the server
     * @param queueLength  the size of the queue to buffer messages. Messages are buffered as a result of the throttle_rate.
     */
    public void subscribe(String topic, String type, RosListenDelegate delegate, int throttleRate, int queueLength) {

        this.subscribe(SubscriptionRequestMsg.generate(topic)
                        .setType(type)
                        .setThrottleRate(throttleRate)
                        .setQueueLength(queueLength),
                delegate);

    }

    /**
     * Subscribes to a topic with the subscription parameters specified in the provided {@link SubscriptionRequestMsg}.
     * The {@link RosListenDelegate} will be notified every time there is a publish to the specified topic.
     *
     * @param request  the subscription request details.
     * @param delegate the delegate that will receive messages each time a message is published to the topic.
     */
    public void subscribe(SubscriptionRequestMsg request, RosListenDelegate delegate) {

        if (this.session == null) {
            throw new RuntimeException("Rosbridge connection is closed. Cannot subscribe.");
        }

        final String topic = request.getTopic();

        //already have a subscription? just update delegate
        synchronized (this.listeners) {
            RosBridgeSubscriber subscriber = this.listeners.get(topic);
            if (subscriber != null) {
                subscriber.addDelegate(delegate);
                return;
            }

            //otherwise setup the subscription and delegate
            this.listeners.put(topic, new RosBridgeSubscriber(delegate));
        }

        final String subMsg = request.generateJsonString();

        try {
            final Future<Void> fut = session.getRemote().sendStringByFuture(subMsg);
            fut.get(2, TimeUnit.SECONDS);
        } catch (final Throwable throwable) {
            final String msg = "Error in sending subscription message to Rosbridge host for topic " + topic;
            LOGGER.error(msg + " " + ExceptionUtils.getStackTrace(throwable));
            throw new RuntimeException(throwable);
        }


    }


    /**
     * Stops a {@link RosListenDelegate} from receiving messages from Rosbridge.
     *
     * @param topic    the topic on which the listener subscribed.
     * @param delegate the delegate to remove.
     */
    public void removeListener(String topic, RosListenDelegate delegate) {

        final RosBridgeSubscriber subscriber = this.listeners.get(topic);
        if (subscriber != null) {
            subscriber.removeDelegate(delegate);

            if (subscriber.numDelegates() == 0) {
                this.unsubscribe(topic);
            }

        }

    }


    /**
     * Advertises that this object will be publishing to a ROS topic.
     *
     * @param topic the topic to which this object will be publishing.
     * @param type  the ROS message type of the topic.
     */
    public void advertise(String topic, String type) {

        if (this.session == null) {
            throw new RuntimeException("Rosbridge connection is closed. Cannot advertise topic: " + topic);
        }

        boolean advertised = false;
        synchronized (this.publishedTopics) {
            advertised = this.publishedTopics.contains(topic);
            if (!advertised)
                this.publishedTopics.add(topic);
        }
        if (!advertised) {

            //then start advertising first
            final String adMsg = "{" +
                    "\"op\": \"advertise\",\n" +
                    "\"topic\": \"" + topic + "\",\n" +
                    "\"type\": \"" + type + "\"\n" +
                    "}";


            try {
                final Future<Void> fut = session.getRemote().sendStringByFuture(adMsg);
                fut.get(2, TimeUnit.SECONDS);
            } catch (final Throwable throwable) {
                this.publishedTopics.remove(topic);
                final String msg = "Error in setting up advertisement to " + topic + " with message type: " + type;
                LOGGER.error(msg + " " + ExceptionUtils.getStackTrace(throwable));
                throw new RuntimeException(throwable);
            }

        }

    }

    /**
     * Unsubscribes from a topic. Note that if there are multiple {@link RosListenDelegate}
     * objects subscribed to a topic, they will all unsubscribe. If you want to remove only
     * one, instead use {@link #removeListener(String, RosListenDelegate)}.
     *
     * @param topic the topic from which to unsubscribe.
     */
    public void unsubscribe(String topic) {

        if (this.session == null) {
            throw new RuntimeException("Rosbridge connection is closed. Cannot unsubscribe. Attempted unsubscribe topic: " + topic);
        }

        final String usMsg = "{" +
                "\"op\": \"unsubscribe\",\n" +
                "\"topic\": \"" + topic + "\"\n" +
                "}";

        try {
            final Future<Void> fut = session.getRemote().sendStringByFuture(usMsg);
            fut.get(2, TimeUnit.SECONDS);
        } catch (final Throwable throwable) {
            final String msg = "Error in sending unsubscribe message for " + topic;
            LOGGER.error(msg + " " + ExceptionUtils.getStackTrace(throwable));
        }

        this.listeners.remove(topic);
    }


    /**
     * Unsubscribes from all topics.
     */
    public void unsubscribeAll() {
        final List<String> curTopics = new ArrayList<String>(this.listeners.keySet());
        for (final String topic : curTopics) {
            this.unsubscribe(topic);
        }
    }


    /**
     * "Unadvertises" that you are publishing to a topic.
     *
     * @param topic the topic to unadvertise
     */
    public void unadvertise(String topic) {

        if (this.session == null) {
            throw new RuntimeException("Rosbridge connection is closed. Cannot unadvertise. Attempted unadvertise topic: " + topic);
        }

        final String usMsg = "{" +
                "\"op\": \"unadvertise\",\n" +
                "\"topic\": \"" + topic + "\"\n" +
                "}";


        try {
            final Future<Void> fut = session.getRemote().sendStringByFuture(usMsg);
            fut.get(2, TimeUnit.SECONDS);
        } catch (final Throwable throwable) {
            final String msg = "Error in sending unsubscribe message for " + topic;
            LOGGER.error(msg + " " + ExceptionUtils.getStackTrace(throwable));
            throw new RuntimeException(throwable);
        }

        synchronized (this.publishedTopics) {
            this.publishedTopics.remove(topic);
        }

    }

    /**
     * Unadvertises for all topics currently being published to.
     */
    public void unadvertiseAll() {
        List<String> curPublishedTopics;
        synchronized (this.publishedTopics) {
            curPublishedTopics = new ArrayList<String>(this.publishedTopics);
        }
        for (final String topic : curPublishedTopics) {
            this.unadvertise(topic);
        }
    }

    /**
     * Unadvertises and unsubscribes from all topics.
     */
    public void unsubsribeUnAdvertiseAll() {
        this.unadvertiseAll();
        this.unsubscribeAll();
    }

    /**
     * Publishes to a topic. If the topic has not already been advertised on ros, it will automatically do so.
     *
     * @param topic the topic to publish to
     * @param type  the message type of the topic
     * @param msg   should be a {@link java.util.Map} or a Java Bean, specifying the ROS message
     */
    public void publish(String topic, String type, Object msg) {

        if (this.session == null) {
            throw new RuntimeException("Rosbridge connection is closed. Cannot publish. Attempted Topic Publish: " + topic);
        }

        this.advertise(topic, type);

        final Map<String, Object> jsonMsg = new HashMap<String, java.lang.Object>();
        jsonMsg.put("op", "publish");
        jsonMsg.put("topic", topic);
        jsonMsg.put("type", type);
        jsonMsg.put("msg", msg);

        final JsonFactory jsonFactory = new JsonFactory();
        final StringWriter writer = new StringWriter();
        final JsonGenerator jsonGenerator;
        final ObjectMapper objectMapper = new ObjectMapper();

        try {
            jsonGenerator = jsonFactory.createGenerator(writer);
            objectMapper.writeValue(jsonGenerator, jsonMsg);
        } catch (final Exception exception) {
            LOGGER.error(ExceptionUtils.getStackTrace(exception));
            throw new RuntimeException(exception);
        }

        final String jsonMsgString = writer.toString();

        try {
            final Future<Void> fut = session.getRemote().sendStringByFuture(jsonMsgString);
            fut.get(2, TimeUnit.SECONDS);
        } catch (final Throwable throwable) {
            final String errorMsg = "Error publishing to " + topic + " with message type: " + type;
            LOGGER.error(errorMsg + " " + ExceptionUtils.getStackTrace(throwable));
            throw new RuntimeException(throwable);
        }

    }


    /**
     * Publishes to a topic with a ros message represented in its JSON string form.
     * If the topic has not already been advertised on ros, it will automatically do so.
     *
     * @param topic   the topic to publish to
     * @param type    the message type of the topic
     * @param jsonMsg the JSON string of the ROS message.
     */
    public void publishJsonMsg(String topic, String type, String jsonMsg) {

        if (this.session == null) {
            throw new RuntimeException("Rosbridge connection is closed. Cannot publish. Attempted Topic Publish: " + topic);
        }

        this.advertise(topic, type);

        final String fullMsg = "{\"op\": \"publish\", \"topic\": \"" + topic + "\", \"type\": \"" + type + "\", " +
                "\"msg\": " + jsonMsg + "}";


        try {
            final Future<Void> fut = session.getRemote().sendStringByFuture(fullMsg);
            fut.get(2, TimeUnit.SECONDS);
        } catch (final Throwable throwable) {
            final String msg = "Error publishing to " + topic + " with message type: " + type;
            LOGGER.error(msg + " " + ExceptionUtils.getStackTrace(throwable));
        }

    }


    /**
     * Sends the provided fully specified message to the ROS Bridge server. Since the RosBridge server
     * expects JSON messages, the string message should probably be in JSON format and adhere to the R
     * Rosbridge protocol, but this method will send whatever raw string you provide.
     *
     * @param message the message to send to Rosbridge.
     */
    public void sendRawMessage(String message) {

        if (this.session == null) {
            throw new RuntimeException("Rosbridge connection is closed. Cannot send message.");
        }


        try {
            final Future<Void> fut = session.getRemote().sendStringByFuture(message);
            fut.get(2, TimeUnit.SECONDS);
        } catch (final Throwable throwable) {
            final String msg = "Error sending raw message to RosBridge server: " + message;
            LOGGER.error(msg + " " + ExceptionUtils.getStackTrace(throwable));
            throw new RuntimeException(throwable);
        }

    }

    /**
     * Attempts to turn the the provided object into a JSON message and send it to the ROSBridge server.
     * If the object does not satisfy the Rosbridge protocol, it may have no affect.
     *
     * @param o the object to turn into a JSON message and send.
     */
    public void formatAndSend(Object o) {

        final JsonFactory jsonFactory = new JsonFactory();
        final StringWriter writer = new StringWriter();
        final JsonGenerator jsonGenerator;
        final ObjectMapper objectMapper = new ObjectMapper();

        try {
            jsonGenerator = jsonFactory.createGenerator(writer);
            objectMapper.writeValue(jsonGenerator, o);
        } catch (final Exception exception) {
            final String msg = "Error parsing object into a JSON message.";
            LOGGER.error(msg + " " + ExceptionUtils.getStackTrace(exception));
            throw new RuntimeException(exception);
        }

        final String jsonMsgString = writer.toString();

        try {
            final Future<Void> fut = session.getRemote().sendStringByFuture(jsonMsgString);
            fut.get(2, TimeUnit.SECONDS);
        } catch (final Throwable throwable) {
            final String msg = "Error sending message to RosBridge server: " + jsonMsgString;
            LOGGER.error(msg + " " + ExceptionUtils.getStackTrace(throwable));
            throw new RuntimeException(throwable);
        }

    }


    protected void processFragment(final JsonNode node) {
        final String id = node.get("id").textValue();
        final FragmentManager manager = this.fragmentManagers.computeIfAbsent(id, key -> new FragmentManager(node));
        final boolean complete = manager.updateFragment(node);
        if (complete) {
            final String fullMsg = manager.generateFullMessage();
            this.fragmentManagers.remove(id);
            manager.close();
            this.onMessage(fullMsg);
        }
    }

    /**
     * Class for managing all the listeners that have subscribed to a topic on Rosbridge.
     * Maintains a list of {@link RosListenDelegate} objects and informs them all
     * when a message has been received from Rosbridge.
     */
    public static class RosBridgeSubscriber {

        protected List<RosListenDelegate> delegates = new CopyOnWriteArrayList<RosListenDelegate>();

        public RosBridgeSubscriber() {
        }

        /**
         * Initializes and adds all the input delegates to receive messages.
         *
         * @param delegates the delegates to receive messages.
         */
        public RosBridgeSubscriber(RosListenDelegate... delegates) {
            for (RosListenDelegate delegate : delegates) {
                this.delegates.add(delegate);
            }
        }

        /**
         * Adds a delegate to receive messages from Rosbridge.
         *
         * @param delegate a delegate to receive messages from Rosbridge.
         */
        public void addDelegate(RosListenDelegate delegate) {
            this.delegates.add(delegate);
        }


        /**
         * Removes a delegate from receiving messages from Rosbridge
         *
         * @param delegate the delegate to stop receiving messages.
         */
        public void removeDelegate(RosListenDelegate delegate) {
            this.delegates.remove(delegate);
        }

        /**
         * Receives a new published message to a subscribed topic and informs all listeners.
         *
         * @param data      the {@link com.fasterxml.jackson.databind.JsonNode} containing the JSON data received.
         * @param stringRep the string representation of the JSON object.
         */
        public void receive(JsonNode data, String stringRep) {
            for (RosListenDelegate delegate : delegates) {
                delegate.receive(data, stringRep);
            }
        }

        /**
         * Returns the number of delegates listening to this topic.
         *
         * @return the number of delegates listening to this topic.
         */
        public int numDelegates() {
            return this.delegates.size();
        }

    }

    private static final class FragmentManager implements Closeable {

        private final String id;
        private final int fragmentsNumber;
        private final AtomicReferenceArray<String> fragments;
        private final CopyOnWriteArraySet<Integer> completedFragments = new CopyOnWriteArraySet<>();

        public FragmentManager(final JsonNode fragmentJson) {
            this.fragmentsNumber = fragmentJson.get("total").intValue();
            this.fragments = new AtomicReferenceArray<>(this.fragmentsNumber);
            this.id = fragmentJson.get("id").textValue();
        }

        public final boolean updateFragment(final JsonNode fragmentJson) {
            final String data = fragmentJson.get("data").asText();
            final int num = fragmentJson.get("num").intValue();
            this.fragments.set(num, data);
            this.completedFragments.add(num);
            return this.complete();
        }

        public final boolean complete() {
            return this.completedFragments.size() == this.fragmentsNumber;
        }


        public final String generateFullMessage() {
            if (!this.complete()) {
                throw new RuntimeException("Cannot generate full message from fragments, because not all fragments have arrived.");
            }

            final StringBuilder buf = new StringBuilder(fragments.get(0).length() * this.fragmentsNumber);
            
            for (int i = 0; i < fragmentsNumber; i++) {
                final String fragment = this.fragments.get(i);
                buf.append(fragment);
            }

            return buf.toString();
        }

        /**
         * Clear collections
         */
        @Override
        public final void close() {
            try {
                this.completedFragments.clear();
            } catch (final Exception exception) {
                LOGGER.debug(ExceptionUtils.getStackTrace(exception));
            }
        }
    }

}
