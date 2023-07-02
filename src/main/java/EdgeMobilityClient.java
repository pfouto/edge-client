import babel.BabelMessage;
import babel.BabelMessageSerializer;
import messaging.PersistenceMessage;
import messaging.ReconfigurationMessage;
import messaging.RequestMessage;
import messaging.ResponseMessage;
import org.apache.commons.lang3.tuple.Pair;
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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class EdgeMobilityClient extends DB {

    private static final AtomicInteger initCounter = new AtomicInteger();
    private static final Map<Long, CompletableFuture<ResponseMessage>> responseCallbacks = new ConcurrentHashMap<>();
    private static final Map<Long, CompletableFuture<PersistenceMessage>> persistenceCallbacks = new ConcurrentHashMap<>();
    private static final AtomicLong idCounter = new AtomicLong();

    private final static ReentrantLock migrationLock = new ReentrantLock();

    private static short persistence;
    private static int timeoutMillis;
    private static int migrationTimeoutMillis;

    private static List<Pair<Integer, String>> path;

    //Channel parameters
    private static BabelMessageSerializer serializer;
    private static Properties channelProps;

    //Client specific state
    private int threadId;
    private HybridTimestamp localClock = new HybridTimestamp(0, 0);

    private ChannelHandler lastHandler;

    private static ChannelHandler handler;

    private static long startTime;


    @Override
    public void init() {
        try {
            //System.err.println(i1 + " " + Thread.currentThread().toString());
            synchronized (initCounter) {
                if (handler == null) {
                    System.err.println("Arguments: " + getProperties());

                    //ONCE
                    timeoutMillis = Integer.parseInt(getProperties().getProperty("timeout_millis", "5000"));
                    migrationTimeoutMillis = Integer.parseInt(getProperties().getProperty("migration_timeout_millis", "5000"));
                    persistence = Short.parseShort(getProperties().getProperty("persistence", "0"));
                    String pathString = getProperties().getProperty("path");
                    path = new ArrayList<>();
                    for (String hop : pathString.split(",")) {
                        String[] parts = hop.split(":");
                        path.add(Pair.of(Integer.parseInt(parts[0]), parts[1]));
                    }
                    System.err.println("Path: " + path);

                    serializer = new BabelMessageSerializer(new HashMap<>());
                    //Serializer.register...
                    serializer.registerProtoSerializer(RequestMessage.ID, RequestMessage.serializer);
                    serializer.registerProtoSerializer(ResponseMessage.ID, ResponseMessage.serializer);
                    serializer.registerProtoSerializer(PersistenceMessage.ID, PersistenceMessage.serializer);
                    serializer.registerProtoSerializer(ReconfigurationMessage.ID, ReconfigurationMessage.serializer);

                    channelProps = new Properties();
                    channelProps.put(SimpleClientChannel.ADDRESS_KEY, path.get(0).getValue());
                    channelProps.put(SimpleClientChannel.PORT_KEY, "2400");
                    channelProps.put(SimpleClientChannel.CONNECT_TIMEOUT_KEY, "10000");
                    channelProps.put(SimpleClientChannel.HEARTBEAT_INTERVAL_KEY, "0");
                    channelProps.put(SimpleClientChannel.HEARTBEAT_TOLERANCE_KEY, "0");

                    CompletableFuture<ChannelEvent> channelFuture = new CompletableFuture<>();
                    handler = new ChannelHandler(path.get(0).getValue(), channelFuture);
                    handler.channel = new SimpleClientChannel<>(serializer, handler, channelProps);
                    handler.channel.openConnection(null);

                    ChannelEvent connectionResult = channelFuture.get();
                    if (connectionResult instanceof ServerUpEvent) {
                        System.err.println("Connected to server " + ((ServerUpEvent) connectionResult).getServer());
                    } else {
                        System.err.println("Connection results " + connectionResult);
                        System.exit(1);
                    }
                    startTime = System.currentTimeMillis();
                    //END ONCE ----------
                }
                threadId = initCounter.getAndIncrement();
                System.err.println("Thread " + threadId + " started");
                lastHandler = handler;
            }
        } catch (UnknownHostException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        try {
            Operation op = new Operation.ReadOperation(table, key);
            return executeOperation(op);
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
            Operation op = new Operation.WriteOperation(table, key, value, persistence);
            return executeOperation(op);
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

    private String hostByTime(long currentTIme) {
        long timeSecs = currentTIme / 1000;
        int i = 0;
        while (i < path.size() - 1 && timeSecs >= path.get(i + 1).getKey()) {
            i++;
        }
        return path.get(i).getValue();
    }

    private Status executeOperation(Operation op) throws InterruptedException, ExecutionException {

        long currentTime = System.currentTimeMillis();
        String host = hostByTime(currentTime - startTime);

        //New host
        if (!host.equals(handler.host)) {
            migrationLock.lock();
            System.err.println("Thread " + threadId + " entered lock");
            //Check again in case another thread already created the connection
            if (!host.equals(handler.host)) {
                System.err.println((currentTime - startTime) / 1000 + " secs: Changing to host " + host);
                newHost(host);
            }
            migrationLock.unlock();
            System.err.println("Thread " + threadId + " left lock");
        }

        //Store reference in case another thread creates a new connection while we are executing our operation
        ChannelHandler currentHandler = handler;

        //Migration
        if (currentHandler != lastHandler) {
            long migId = idCounter.incrementAndGet();
            RequestMessage migMsg = new RequestMessage(migId, new Operation.MigrationOperation(localClock, lastHandler.currentBranch));
            CompletableFuture<ResponseMessage> future = new CompletableFuture<>();
            responseCallbacks.put(migId, future);
            currentHandler.channel.sendMessage(new BabelMessage(migMsg, (short) 400, (short) 400), null, 0);

            try {
                future.get(migrationTimeoutMillis, TimeUnit.MILLISECONDS);
            } catch (TimeoutException ex) {
                System.err.println("Migration op timed out to " + currentHandler.host + " " + migMsg);
                System.exit(1);
                return Status.SERVICE_UNAVAILABLE;
            }
            System.err.println("Thread " + threadId + " migrated to "  +currentHandler.host );
            lastHandler = currentHandler;
        }

        //Operation
        long opId = idCounter.incrementAndGet();
        RequestMessage requestMessage = new RequestMessage(opId, op);
        CompletableFuture<ResponseMessage> future = new CompletableFuture<>();
        responseCallbacks.put(opId, future);

        //Create persistence callback if configured
        CompletableFuture<PersistenceMessage> persistFuture = null;
        if (requestMessage.getOp().getType() == Operation.WRITE && persistence > 0) {
            persistFuture = new CompletableFuture<>();
            persistenceCallbacks.put(requestMessage.getOpId(), persistFuture);
        }

        currentHandler.channel.sendMessage(new BabelMessage(requestMessage, (short) 400, (short) 400), null, 0);
        try {
            ResponseMessage resp = future.get(timeoutMillis, TimeUnit.MILLISECONDS);

            //Handle clock
            if (resp.getHlc() != null)
                localClock = localClock.max(resp.getHlc());

            //Maybe wait for persistence
            if (requestMessage.getOp().getType() == Operation.WRITE && persistence > 0) {
                persistFuture.get(timeoutMillis, TimeUnit.MILLISECONDS);
            }
            return Status.OK;

        } catch (TimeoutException ex) {
            System.err.println("Op Timed out..." + requestMessage);
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

    static void newHost(String host) {
        try {
            channelProps.put(SimpleClientChannel.ADDRESS_KEY, host);

            CompletableFuture<ChannelEvent> channelFuture = new CompletableFuture<>();
            ChannelHandler newHandler = new ChannelHandler(host, channelFuture);
            newHandler.channel = new SimpleClientChannel<>(serializer, newHandler, channelProps);
            newHandler.channel.openConnection(null);

            ChannelEvent connectionResult = channelFuture.get();
            if (connectionResult instanceof ServerUpEvent) {
                System.err.println("Changed connection to server " + ((ServerUpEvent) connectionResult).getServer());
                handler = newHandler;
            } else {
                System.err.println("Failed to connect to new server" + connectionResult);
                System.exit(1);
            }
        } catch (Exception e) {
            System.err.println("Error connecting to new server");
            e.printStackTrace();
            System.exit(1);
        }
    }

    static class ChannelHandler implements ChannelListener<BabelMessage> {
        //Singleton class, methods called by channel thread
        private final CompletableFuture<ChannelEvent> channelFuture;
        List<Host> currentBranch = new ArrayList<>();

        SimpleClientChannel<BabelMessage> channel = null;

        final String host;

        public ChannelHandler(String host, CompletableFuture<ChannelEvent> channelFuture) {
            this.channelFuture = channelFuture;
            this.host = host;
        }

        @Override
        public void deliverMessage(BabelMessage msg, Host from) {
            if (msg.getMessage() instanceof ResponseMessage) {
                ResponseMessage message = (ResponseMessage) msg.getMessage();
                responseCallbacks.remove(message.getOpId()).complete(message);
            } else if (msg.getMessage() instanceof PersistenceMessage) {
                PersistenceMessage message = (PersistenceMessage) msg.getMessage();
                if (persistence > 0)
                    persistenceCallbacks.remove(message.getOpId()).complete(message);
            } else if (msg.getMessage() instanceof ReconfigurationMessage) {
                ReconfigurationMessage message = (ReconfigurationMessage) msg.getMessage();
                currentBranch = message.getHosts();
                System.err.println("New branch " + currentBranch);
            } else {
                System.err.println("Unknown message type!");
                System.exit(1);
            }
        }

        @Override
        public void deliverEvent(ChannelEvent evt) {
            if (evt instanceof ServerUpEvent)
                channelFuture.complete(evt);
            else if (evt instanceof ServerDownEvent) {
                System.err.println("Server down! " + ((ServerDownEvent) evt).getCause());
                System.exit(1);
            } else if (evt instanceof ServerFailedEvent) {
                channelFuture.complete(evt);
            } else {
                System.err.println("Unknown event!");
                System.exit(1);
            }
        }

        @Override
        public void messageSent(BabelMessage msg, Host to) {

        }

        @Override
        public void messageFailed(BabelMessage msg, Host to, Throwable cause) {
            System.err.println("Message " + msg + " failed to " + to + " " + cause);
        }

    }
}
