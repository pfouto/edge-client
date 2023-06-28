import babel.BabelMessage;
import babel.BabelMessageSerializer;
import messaging.*;
import pt.unl.fct.di.novasys.channel.ChannelEvent;
import pt.unl.fct.di.novasys.channel.ChannelListener;
import pt.unl.fct.di.novasys.channel.simpleclientserver.SimpleClientChannel;
import pt.unl.fct.di.novasys.channel.simpleclientserver.events.ServerDownEvent;
import pt.unl.fct.di.novasys.channel.simpleclientserver.events.ServerFailedEvent;
import pt.unl.fct.di.novasys.channel.simpleclientserver.events.ServerUpEvent;
import pt.unl.fct.di.novasys.network.data.Host;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.Status;
import utils.HybridTimestamp;
import utils.Operation;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class EdgeClient extends DB {

    private static final AtomicInteger initCounter = new AtomicInteger();
    private static final Map<Long, CompletableFuture<ResponseMessage>> responseCallbacks = new ConcurrentHashMap<>();
    private static final Map<Long, CompletableFuture<PersistenceMessage>> persistenceCallbacks = new ConcurrentHashMap<>();
    private static final AtomicLong idCounter = new AtomicLong();
    private static SimpleClientChannel<BabelMessage> channel = null;

    private static CompletableFuture<ServerUpEvent> channelFuture = null;

    private static short persistence;
    private static int timeoutMillis;

    private static boolean blockPersistence;


    private HybridTimestamp localClock = new HybridTimestamp(0, 0);
    private static List<Host> currentHosts = new ArrayList<>();

    @Override
    public void init() {
        try {

            //System.err.println(i1 + " " + Thread.currentThread().toString());
            synchronized (initCounter) {
                if (channel == null) {
                    System.err.println("Arguments: " + getProperties());
                    //ONCE
                    timeoutMillis = Integer.parseInt(getProperties().getProperty("timeout_millis", "5000"));
                    blockPersistence = Boolean.parseBoolean(getProperties().getProperty("block_persistence", "false"));
                    persistence = Short.parseShort(getProperties().getProperty("persistence", "0"));
                    String host = getProperties().getProperty("host");

                    BabelMessageSerializer serializer = new BabelMessageSerializer(new HashMap<>());
                    //Serializer.register...
                    serializer.registerProtoSerializer(RequestMessage.ID, RequestMessage.serializer);
                    serializer.registerProtoSerializer(ResponseMessage.ID, ResponseMessage.serializer);
                    serializer.registerProtoSerializer(PersistenceMessage.ID, PersistenceMessage.serializer);
                    serializer.registerProtoSerializer(ReconfigurationMessage.ID, ReconfigurationMessage.serializer);

                    Properties props = new Properties();
                    props.put(SimpleClientChannel.ADDRESS_KEY, host);
                    props.put(SimpleClientChannel.PORT_KEY, "2400");
                    props.put(SimpleClientChannel.CONNECT_TIMEOUT_KEY, "10000");
                    props.put(SimpleClientChannel.HEARTBEAT_INTERVAL_KEY, "0");
                    props.put(SimpleClientChannel.HEARTBEAT_TOLERANCE_KEY, "0");
                    channel = new SimpleClientChannel<>(serializer, new ChannelHandler(), props);

                    channelFuture = new CompletableFuture<>();
                    channel.openConnection(null);

                    ServerUpEvent serverUpEvent = channelFuture.get();
                    System.err.println("Connected to server " + serverUpEvent.getServer());

                    //END ONCE ----------
                }
                int threadId = initCounter.getAndIncrement();
                //System.err.println("Thread " + threadId + " started");
            }
        } catch (UnknownHostException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        try {
            long id = idCounter.incrementAndGet();
            RequestMessage requestMessage = new RequestMessage(id, new Operation.ReadOperation(table, key));
            return executeOperation(requestMessage);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
            return Status.ERROR;
        }
    }

    @Override
    public Status update(String table, String key, Map<String, ByteIterator> values) {
        try {
            byte[] value = values.values().iterator().next().toArray();
            long id = idCounter.incrementAndGet();
            RequestMessage requestMessage = new RequestMessage(id, new Operation.WriteOperation(table, key, value, persistence));
            return executeOperation(requestMessage);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
            return Status.ERROR;
        }
    }

    @Override
    public Status insert(String table, String key, Map<String, ByteIterator> values) {
        throw new AssertionError();
    }

    private Status executeOperation(RequestMessage requestMessage) throws InterruptedException, ExecutionException {
        CompletableFuture<ResponseMessage> future = new CompletableFuture<>();
        responseCallbacks.put(requestMessage.getOpId(), future);

        //Create persistence callback if configured
        CompletableFuture<PersistenceMessage> persistFuture = null;
        if (requestMessage.getOp().getType() == Operation.WRITE && blockPersistence) {
            persistFuture = new CompletableFuture<>();
            persistenceCallbacks.put(requestMessage.getOpId(), persistFuture);
        }

        channel.sendMessage(new BabelMessage(requestMessage, (short) 400, (short) 400), null, 0);
        try {
            ResponseMessage resp = future.get(timeoutMillis, TimeUnit.MILLISECONDS);
            //Handle clock
            if (resp.getHlc() != null)
                localClock = localClock.max(resp.getHlc());

            //Maybe wait for persistence
            if (requestMessage.getOp().getType() == Operation.WRITE && blockPersistence) {
                persistFuture.get(timeoutMillis, TimeUnit.MILLISECONDS);
            }
            return Status.OK;
        } catch (TimeoutException ex) {
            System.err.println("Op Timed out..." + requestMessage.getOpId() + " " +  requestMessage.getOp());
            ex.printStackTrace();
            System.exit(1);
            return Status.SERVICE_UNAVAILABLE;
        }
    }

    @Override
    public Status scan(String t, String sK, int rC, Set<String> f, Vector<HashMap<String, ByteIterator>> res) {
        throw new AssertionError();
    }

    @Override
    public Status delete(String table, String key) {
        throw new AssertionError();
    }

    static class ChannelHandler implements ChannelListener<BabelMessage> {
        //Singleton class, methods called by channel thread

        @Override
        public void deliverMessage(BabelMessage msg, Host from) {
            if (msg.getMessage() instanceof ResponseMessage) {
                ResponseMessage message = (ResponseMessage) msg.getMessage();
                responseCallbacks.remove(message.getOpId()).complete(message);
            } else if (msg.getMessage() instanceof PersistenceMessage) {
                PersistenceMessage message = (PersistenceMessage) msg.getMessage();
                if (blockPersistence)
                    persistenceCallbacks.remove(message.getOpId()).complete(message);
            } else if (msg.getMessage() instanceof ReconfigurationMessage) {
                ReconfigurationMessage message = (ReconfigurationMessage) msg.getMessage();
                currentHosts = message.getHosts();
            } else {
                System.err.println("Unknown message type!");
                System.exit(1);
            }
        }

        @Override
        public void messageSent(BabelMessage msg, Host to) {

        }

        @Override
        public void messageFailed(BabelMessage msg, Host to, Throwable cause) {
            System.err.println("Message " + msg + " failed to " + to);
        }

        @Override
        public void deliverEvent(ChannelEvent evt) {
            if (evt instanceof ServerUpEvent)
                channelFuture.complete((ServerUpEvent) evt);
            else if (evt instanceof ServerDownEvent) {
                System.err.println("Server down! " + ((ServerDownEvent) evt).getCause());
                System.exit(1);
            } else if (evt instanceof ServerFailedEvent) {
                System.err.println("Server failed! " + ((ServerFailedEvent) evt).getCause());
                System.exit(1);
            } else {
                System.err.println("Unknown event!");
                System.exit(1);
            }
        }

    }
}
