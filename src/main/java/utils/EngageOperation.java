package utils;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class EngageOperation {

    public static final short READ = 1;
    public static final short WRITE = 2;
    private final short type;

    public EngageOperation(short type) {
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

    public static ISerializer<EngageOperation> serializer = new ISerializer<EngageOperation>() {

        @Override
        public void serialize(EngageOperation o, ByteBuf out) throws IOException {
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
                    Clock.serializer.serialize(write.getClock(), out);
                    break;
            }
        }

        @Override
        public EngageOperation deserialize(ByteBuf in) throws IOException {
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
                    Clock c = Clock.serializer.deserialize(in);
                    return new WriteOperation(writePartition, writeKey, value, c);
            }
            throw new IOException("Unknown operation type: " + type);
        }
    };

    public static class ReadOperation extends EngageOperation {
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

        @Override
        public String toString() {
            return "ReadOperation{" +
                    "partition='" + partition + '\'' +
                    ", key='" + key + '\'' +
                    '}';
        }
    }

    public static class WriteOperation extends EngageOperation {
        private final String partition;
        private final String key;
        private final byte[] value;
        private final Clock clock;

        public WriteOperation(String partition, String key, byte[] value, Clock clock) {
            super(WRITE);
            this.partition = partition;
            this.key = key;
            this.value = value;
            this.clock = clock;
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

        @Override
        public String toString() {
            return "WriteOperation{" +
                    "partition='" + partition + '\'' +
                    ", key='" + key + '\'' +
                    ", value=" + value.length +
                    '}';
        }

        public Clock getClock() {
            return clock;
        }
    }
}
