package com.stoliar.service_grpc.service;

import com.stoliar.service_grpc.repository.KvRepository;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

@Service
public class KvService {

    private final KvRepository repository;

    public KvService(KvRepository repository) {
        this.repository = repository;
    }

    public void put(String key, byte[] value) {
        repository.put(key, value);
    }

    public Optional<byte[]> get(String key) {
        return repository.get(key);
    }

    public void delete(String key) {
        repository.delete(key);
    }

    public void rangeStream(String from, String to, Consumer<Object[]> consumer) {

        String currentKey = from;
        int batchSize = 1000;

        while (true) {
            List<Object[]> batch = repository.rangeBatch(currentKey, batchSize);

            if (batch.isEmpty()) break;

            for (Object[] tuple : batch) {
                String key = (String) tuple[0];

                // skip duplicate cursor
                if (key.equals(currentKey)) continue;

                if (key.compareTo(to) > 0) return;

                consumer.accept(tuple);
                currentKey = key;
            }

            if (batch.size() < batchSize) break;
        }
    }

    public long count() {
        return repository.count();
    }
}