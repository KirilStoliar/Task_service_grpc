package com.stoliar.service_grpc.tarantool;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessageUnpacker;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class TarantoolKvClient {

    private final String host;
    private final int port;
    private int syncId = 0;
    private static final String SPACE = "KV"; // фиксированное пространство

    public TarantoolKvClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    private synchronized byte[] executeRequest(byte[] request) {
        try (Socket socket = new Socket(host, port)) {
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            out.write(request);
            out.flush();

            byte[] buffer = new byte[65536];
            int totalRead = 0;
            int bytesRead;

            while ((bytesRead = in.read(buffer, totalRead, buffer.length - totalRead)) != -1) {
                totalRead += bytesRead;
                if (totalRead == buffer.length) {
                    byte[] newBuffer = new byte[buffer.length * 2];
                    System.arraycopy(buffer, 0, newBuffer, 0, buffer.length);
                    buffer = newBuffer;
                }
                if (bytesRead < 1024) break; // Небольшая пауза для завершения
            }

            byte[] result = new byte[totalRead];
            System.arraycopy(buffer, 0, result, 0, totalRead);
            return result;

        } catch (Exception e) {
            throw new RuntimeException("Failed to execute request", e);
        }
    }

    // PUT
    public void put(String key, byte[] value) {
        try {
            MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
            packer.packInt(++syncId);
            packer.packInt(0x02); // REPLACE

            packer.packMapHeader(2);
            packer.packInt(0x10); // space
            packer.packString(SPACE);
            packer.packInt(0x21); // tuple
            packer.packArrayHeader(2);
            packer.packString(key);
            if (value == null) packer.packNil();
            else {
                packer.packBinaryHeader(value.length);
                packer.writePayload(value);
            }
            packer.close();

            executeRequest(packer.toByteArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // GET
    public Object[] get(String key) {
        try {
            MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
            packer.packInt(++syncId);
            packer.packInt(0x01); // SELECT
            packer.packMapHeader(4);
            packer.packInt(0x10); packer.packString(SPACE); // space
            packer.packInt(0x11); packer.packInt(0); // index
            packer.packInt(0x20); packer.packArrayHeader(1); packer.packString(key); // key
            packer.packInt(0x12); packer.packInt(1); // limit
            packer.close();

            byte[] resp = executeRequest(packer.toByteArray());

            try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(resp)) {
                unpacker.unpackInt(); // sync
                unpacker.unpackInt(); // code
                int mapSize = unpacker.unpackMapHeader();
                for (int i = 0; i < mapSize; i++) {
                    int k = unpacker.unpackInt();
                    if (k == 0x30) {
                        int count = unpacker.unpackArrayHeader();
                        if (count == 0) return null;
                        int fields = unpacker.unpackArrayHeader();
                        String rKey = unpacker.unpackString();
                        byte[] rValue = null;
                        if (!unpacker.tryUnpackNil()) {
                            int len = unpacker.unpackBinaryHeader();
                            rValue = new byte[len];
                            unpacker.readPayload(rValue);
                        }
                        return new Object[]{rKey, rValue};
                    } else unpacker.skipValue();
                }
            }
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // DELETE
    public void delete(String key) {
        try {
            MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
            packer.packInt(++syncId);
            packer.packInt(0x03); // DELETE
            packer.packMapHeader(2);
            packer.packInt(0x10); packer.packString(SPACE);
            packer.packInt(0x22); packer.packArrayHeader(1); packer.packString(key);
            packer.close();

            executeRequest(packer.toByteArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // COUNT
    public long count() {
        try {
            MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
            packer.packInt(++syncId);
            packer.packInt(0x0D); // CALL
            packer.packMapHeader(2);
            packer.packInt(0x11); packer.packString("box.space." + SPACE + ":len");
            packer.packInt(0x21); packer.packArrayHeader(0);
            packer.close();

            byte[] resp = executeRequest(packer.toByteArray());

            try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(resp)) {
                unpacker.unpackInt(); // sync
                unpacker.unpackInt(); // code
                int mapSize = unpacker.unpackMapHeader();
                for (int i = 0; i < mapSize; i++) {
                    int k = unpacker.unpackInt();
                    if (k == 0x30) {
                        unpacker.unpackArrayHeader(); // count array
                        return unpacker.unpackLong();
                    } else unpacker.skipValue();
                }
            }
            return 0;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // RANGE
    public List<Object[]> rangeBatch(String from, int limit) {
        try {
            MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();

            packer.packInt(++syncId);
            packer.packInt(0x01);

            packer.packMapHeader(5);
            packer.packInt(0x10); packer.packString(SPACE);
            packer.packInt(0x11); packer.packInt(0);
            packer.packInt(0x20); packer.packArrayHeader(1); packer.packString(from);
            packer.packInt(0x12); packer.packInt(limit);
            packer.packInt(0x13); packer.packInt(2); // GE

            packer.close();

            byte[] resp = executeRequest(packer.toByteArray());
            List<Object[]> result = new ArrayList<>();

            try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(resp)) {
                unpacker.unpackInt();
                unpacker.unpackInt();

                int mapSize = unpacker.unpackMapHeader();
                for (int i = 0; i < mapSize; i++) {
                    int k = unpacker.unpackInt();
                    if (k == 0x30) {
                        int tupleCount = unpacker.unpackArrayHeader();

                        for (int j = 0; j < tupleCount; j++) {
                            unpacker.unpackArrayHeader();

                            String key = unpacker.unpackString();

                            byte[] value = null;
                            if (!unpacker.tryUnpackNil()) {
                                int len = unpacker.unpackBinaryHeader();
                                value = new byte[len];
                                unpacker.readPayload(value);
                            }

                            result.add(new Object[]{key, value});
                        }
                    } else unpacker.skipValue();
                }
            }

            return result;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}