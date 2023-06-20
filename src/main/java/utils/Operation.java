package utils;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class Operation {

    public static final short READ = 1;
    public static final short WRITE = 2;
    public static final short MIGRATION = 3;
    public static final short PARTITION_FETCH = 4;
    private final short type;

    public Operation(short type) {
        this.type = type;
    }

    public short getType() {
        return type;
    }

    private static void encodeUTF8(String str, ByteBuf out) {
        byte[] stringBytes = str.getBytes(StandardCharsets.UTF_8);
        out.writeInt(stringBytes.length);
        out.writeBytes(stringBytes);
    }

    private static String decodeUTF8(ByteBuf in) {
        int length = in.readInt();
        byte[] stringBytes = new byte[length];
        in.readBytes(stringBytes);
        return new String(stringBytes, StandardCharsets.UTF_8);
    }

    public static ISerializer<Operation> serializer = new ISerializer<Operation>() {

        @Override
        public void serialize(Operation o, ByteBuf out) throws IOException {
            out.writeShort(o.getType());
            switch (o.getType()) {
                case READ:
                    ReadOperation read = (ReadOperation) o;
                    encodeUTF8(read.getPartition(), out);
                    encodeUTF8(read.getKey(), out);
                    break;
                case WRITE:
                    WriteOperation write = (WriteOperation) o;
                    encodeUTF8(write.getPartition(), out);
                    encodeUTF8(write.getKey(), out);
                    out.writeInt(write.getValue().length);
                    out.writeBytes(write.getValue());
                    out.writeShort(write.getPersistence());
                    break;
                case MIGRATION:
                    MigrationOperation migration = (MigrationOperation) o;
                    HybridTimestamp.serializer.serialize(migration.getHlc(), out);
                    out.writeInt(migration.path.size());
                    for (Host h : migration.path) {
                        Host.serializer.serialize(h, out);
                    }
                    break;
                case PARTITION_FETCH:
                    PartitionFetchOperation fetch = (PartitionFetchOperation) o;
                    encodeUTF8(fetch.getPartition(), out);
                    break;
            }
        }

        @Override
        public Operation deserialize(ByteBuf in) throws IOException {
            short type = in.readShort();
            switch (type) {
                case READ:
                    String readPartition = decodeUTF8(in);
                    String readKey = decodeUTF8(in);
                    return new ReadOperation(readPartition, readKey);
                case WRITE:
                    String writePartition = decodeUTF8(in);
                    String writeKey = decodeUTF8(in);
                    int length = in.readInt();
                    byte[] value = new byte[length];
                    in.readBytes(value);
                    short persistence = in.readShort();
                    return new WriteOperation(writePartition, writeKey, value, persistence);
                case MIGRATION:
                    HybridTimestamp hlc = HybridTimestamp.serializer.deserialize(in);
                    int size = in.readInt();
                    List<Host> path = new ArrayList<>(size);
                    for (int i = 0; i < size; i++) {
                        path.add(Host.serializer.deserialize(in));
                    }
                    return new MigrationOperation(hlc, path);
                case PARTITION_FETCH:
                    String fetchPartition = decodeUTF8(in);
                    return new PartitionFetchOperation(fetchPartition);
            }
            throw new IOException("Unknown operation type: " + type);
        }
    };

    public static class ReadOperation extends Operation {
        private final String partition;
        private final String key;

        public ReadOperation(String partition, String key) {
            super(READ);
            this.partition = partition;
            this.key = key;
        }

        public String getPartition() {
            return partition;
        }

        public String getKey() {
            return key;
        }
    }

    public static class WriteOperation extends Operation {
        private final String partition;
        private final String key;
        private final byte[] value;
        private final short persistence;

        public WriteOperation(String partition, String key, byte[] value, short persistence) {
            super(WRITE);
            this.partition = partition;
            this.key = key;
            this.value = value;
            this.persistence = persistence;
        }

        public String getPartition() {
            return partition;
        }

        public String getKey() {
            return key;
        }

        public byte[] getValue() {
            return value;
        }

        public short getPersistence() {
            return persistence;
        }

    }

    public static class MigrationOperation extends Operation {
        private final HybridTimestamp hlc;
        private final List<Host> path;

        public MigrationOperation(HybridTimestamp hlc, List<Host> path) {
            super(MIGRATION);
            this.hlc = hlc;
            this.path = path;
        }

        public HybridTimestamp getHlc() {
            return hlc;
        }

        public List<Host> getPath() {
            return path;
        }
    }

    public static class PartitionFetchOperation extends Operation {
        private final String partition;

        public PartitionFetchOperation(String partition) {
            super(PARTITION_FETCH);
            this.partition = partition;
        }

        public String getPartition() {
            return partition;
        }
    }

}
