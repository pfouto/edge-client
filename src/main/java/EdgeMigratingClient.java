import babel.BabelMessage;
import babel.BabelMessageSerializer;
import messaging.PersistenceMessage;
import messaging.ReconfigurationMessage;
import messaging.RequestMessage;
import messaging.ResponseMessage;
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
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class EdgeMigratingClient extends DB {

    private static final AtomicInteger initCounter = new AtomicInteger();
    private static final Map<Long, CompletableFuture<Optional<ResponseMessage>>> responseCallbacks = new ConcurrentHashMap<>();
    private static final Map<Long, CompletableFuture<Optional<PersistenceMessage>>> persistenceCallbacks = new ConcurrentHashMap<>();
    private static final AtomicLong idCounter = new AtomicLong();

    private static List<Host> currentBranch = new ArrayList<>();
    private static List<Host> oldBranch = new ArrayList<>();

    private static final ReentrantReadWriteLock migrationLock = new ReentrantReadWriteLock(true);


    private static short persistence;
    private static int timeoutMillis;
    private static boolean blockPersistence;

    //Channel parameters
    private static BabelMessageSerializer serializer;
    private static Properties channelProps;
    private static SimpleClientChannel<BabelMessage> channel = null;

    //Client specific state
    private SimpleClientChannel<BabelMessage> lastChannel;
    private int threadId;
    private HybridTimestamp localClock = new HybridTimestamp(0, 0);


    @Override
    public void init() {
        try {
            //System.err.println(i1 + " " + Thread.currentThread().toString());
            synchronized (initCounter) {
                if (channel == null) {
                    System.err.println("Arguments: " + getProperties());

                    //TODO call migrate here

                    //ONCE
                    timeoutMillis = Integer.parseInt(getProperties().getProperty("timeout_millis", "5000"));
                    blockPersistence = Boolean.parseBoolean(getProperties().getProperty("block_persistence", "false"));
                    persistence = Short.parseShort(getProperties().getProperty("persistence", "0"));
                    String host = getProperties().getProperty("host");

                    serializer = new BabelMessageSerializer(new HashMap<>());
                    //Serializer.register...
                    serializer.registerProtoSerializer(RequestMessage.ID, RequestMessage.serializer);
                    serializer.registerProtoSerializer(ResponseMessage.ID, ResponseMessage.serializer);
                    serializer.registerProtoSerializer(PersistenceMessage.ID, PersistenceMessage.serializer);
                    serializer.registerProtoSerializer(ReconfigurationMessage.ID, ReconfigurationMessage.serializer);

                    channelProps = new Properties();
                    channelProps.put(SimpleClientChannel.ADDRESS_KEY, host);
                    channelProps.put(SimpleClientChannel.PORT_KEY, "2400");
                    channelProps.put(SimpleClientChannel.CONNECT_TIMEOUT_KEY, "10000");
                    channelProps.put(SimpleClientChannel.HEARTBEAT_INTERVAL_KEY, "0");
                    channelProps.put(SimpleClientChannel.HEARTBEAT_TOLERANCE_KEY, "0");

                    CompletableFuture<ChannelEvent> channelFuture = new CompletableFuture<>();
                    channel = new SimpleClientChannel<>(serializer, new ChannelHandler(channelFuture), channelProps);
                    channel.openConnection(null);

                    ChannelEvent connectionResult = channelFuture.get();
                    if (connectionResult instanceof ServerUpEvent) {
                        System.err.println("Connected to server " + ((ServerUpEvent) connectionResult).getServer());
                    } else {
                        System.err.println("Connection results " + connectionResult);
                        System.exit(1);
                    }
                    //END ONCE ----------
                }
                threadId = initCounter.getAndIncrement();
                System.err.println("Thread " + threadId + " started");
                lastChannel = channel;
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

    private Status executeOperation(Operation op) throws InterruptedException, ExecutionException {
        try {
            while (true) {
                migrationLock.readLock().lock();

                if (channel != lastChannel) {
                    System.err.println("Thread " + threadId + " detected new channel, migrating...");
                    long migId = idCounter.incrementAndGet();
                    RequestMessage migMsg = new RequestMessage(migId, new Operation.MigrationOperation(localClock, oldBranch));
                    CompletableFuture<Optional<ResponseMessage>> future = new CompletableFuture<>();
                    responseCallbacks.put(migId, future);
                    channel.sendMessage(new BabelMessage(migMsg, (short) 400, (short) 400), null, 0);
                    migrationLock.readLock().unlock();

                    Optional<ResponseMessage> optResp;
                    try {
                        optResp = future.get(timeoutMillis, TimeUnit.MILLISECONDS);
                    } catch (TimeoutException ex) {
                            System.err.println("Op Timed out..." + migMsg);
                            System.exit(1);
                            return Status.SERVICE_UNAVAILABLE;
                    }
                    if (optResp.isPresent()) {
                        System.err.println("Thread " + threadId + " migrated");
                        lastChannel = channel;
                    } else {
                        System.err.println("Thread " + threadId + " migration failed");
                    }
                } else {
                    long opId = idCounter.incrementAndGet();
                    RequestMessage requestMessage = new RequestMessage(opId, op);
                    CompletableFuture<Optional<ResponseMessage>> future = new CompletableFuture<>();
                    responseCallbacks.put(opId, future);

                    //Create persistence callback if configured
                    CompletableFuture<Optional<PersistenceMessage>> persistFuture = null;
                    if (requestMessage.getOp().getType() == Operation.WRITE && blockPersistence) {
                        persistFuture = new CompletableFuture<>();
                        persistenceCallbacks.put(requestMessage.getOpId(), persistFuture);
                    }

                    channel.sendMessage(new BabelMessage(requestMessage, (short) 400, (short) 400), null, 0);
                    migrationLock.readLock().unlock();

                    Optional<ResponseMessage> optResp = future.get(timeoutMillis, TimeUnit.MILLISECONDS);
                    //Handle clock
                    if (optResp.isPresent()) {
                        ResponseMessage resp = optResp.get();

                        if (resp.getHlc() != null)
                            localClock = localClock.max(resp.getHlc());

                        //Maybe wait for persistence
                        if (requestMessage.getOp().getType() == Operation.WRITE && blockPersistence) {
                            Optional<PersistenceMessage> optPer = persistFuture.get(timeoutMillis, TimeUnit.MILLISECONDS);
                            if (optPer.isPresent()) {
                                return Status.OK;
                            } else {
                                System.err.println("Persistence failed, assuming migration " + threadId);
                                //Else will just loop again (blocking until the migration finishes)
                            }
                        } else {
                            return Status.OK;
                        }
                    } else {
                        System.err.println("Op response empty, assuming migration " + threadId);
                        //Else will just loop again (blocking until the migration finishes)
                    }
                }
            }
        } catch (TimeoutException ex) {
            System.err.println("Op Timed out..." + op);
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

    static void migrate() {

        try {
            migrationLock.writeLock().lock();
            oldBranch = new LinkedList<>(currentBranch);

            channel = null;
            while (channel == null) {
                //Complete all futures, which will cause the clients to retry the operation (after the migration lock is released)
                Iterator<Map.Entry<Long, CompletableFuture<Optional<ResponseMessage>>>> respIt = responseCallbacks.entrySet().iterator();
                while (respIt.hasNext()) {
                    respIt.next().getValue().complete(Optional.empty());
                    respIt.remove();
                }
                Iterator<Map.Entry<Long, CompletableFuture<Optional<PersistenceMessage>>>> perIt = persistenceCallbacks.entrySet().iterator();
                while (perIt.hasNext()) {
                    perIt.next().getValue().complete(Optional.empty());
                    perIt.remove();
                }
                currentBranch.remove(0);
                if (currentBranch.isEmpty()) {
                    System.err.println("No more hosts to migrate to");
                    System.exit(1);
                }
                Host nextHost = currentBranch.get(0);
                System.err.println("Migrating to next parent " + nextHost);
                channelProps.put(SimpleClientChannel.ADDRESS_KEY, nextHost.getAddress().getHostAddress());

                CompletableFuture<ChannelEvent> channelFuture = new CompletableFuture<>();
                channel = new SimpleClientChannel<>(serializer, new ChannelHandler(channelFuture), channelProps);
                channel.openConnection(null);

                ChannelEvent connectionResult = channelFuture.get();
                if (connectionResult instanceof ServerUpEvent) {
                    System.err.println("Changed connection to server " + ((ServerUpEvent) connectionResult).getServer());
                } else {
                    channel = null;
                    System.err.println("Failed to migrate" + connectionResult);
                    // Will loop again and try the next host
                }
            }
            migrationLock.writeLock().unlock();
        } catch (Exception e) {
            System.err.println("Error migrating");
            e.printStackTrace();
            System.exit(1);
        }
    }

    static class ChannelHandler implements ChannelListener<BabelMessage> {
        //Singleton class, methods called by channel thread
        private final CompletableFuture<ChannelEvent> channelFuture;

        public ChannelHandler(CompletableFuture<ChannelEvent> channelFuture) {
            this.channelFuture = channelFuture;
        }

        @Override
        public void deliverMessage(BabelMessage msg, Host from) {
            if (msg.getMessage() instanceof ResponseMessage) {
                ResponseMessage message = (ResponseMessage) msg.getMessage();
                responseCallbacks.remove(message.getOpId()).complete(Optional.of(message));
            } else if (msg.getMessage() instanceof PersistenceMessage) {
                PersistenceMessage message = (PersistenceMessage) msg.getMessage();
                if (blockPersistence)
                    persistenceCallbacks.remove(message.getOpId()).complete(Optional.of(message));
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
                migrate();
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
            System.err.println("Message " + msg + " failed to " + to);
        }

    }
}
