package com.stoliar.service_grpc.repository;

import com.stoliar.service_grpc.tarantool.TarantoolKvClient;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public class KvRepository {

    private final TarantoolKvClient client;

    public KvRepository() {
        this.client = new TarantoolKvClient("127.0.0.1", 3301);
    }

    public void put(String key, byte[] value) {
        client.put(key, value);
    }

    public Optional<byte[]> get(String key) {
        Object[] tuple = client.get(key);
        if (tuple == null) {
            return Optional.empty();
        }
        return Optional.ofNullable((byte[]) tuple[1]);
    }

    public void delete(String key) {
        client.delete(key);
    }

    public List<Object[]> rangeBatch(String from, int limit) {
        return client.rangeBatch(from, limit);
    }

    public long count() {
        return client.count();
    }
}