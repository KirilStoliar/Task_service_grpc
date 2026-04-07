package com.stoliar.service_grpc.grpc;

import com.stoliar.service_grpc.kv.*;
import com.stoliar.service_grpc.service.KvService;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class KvGrpcService extends KvServiceGrpc.KvServiceImplBase {

    private final KvService service;

    public KvGrpcService(KvService service) {
        this.service = service;
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        service.put(request.getKey(), request.getIsNull() ? null : request.getValue().toByteArray());
        responseObserver.onNext(PutResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        GetResponse.Builder response = GetResponse.newBuilder();
        service.get(request.getKey()).ifPresentOrElse(
                value -> {
                    response.setValue(com.google.protobuf.ByteString.copyFrom(value));
                    response.setIsNull(false);
                },
                () -> response.setIsNull(true)
        );
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        service.delete(request.getKey());
        responseObserver.onNext(DeleteResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void range(RangeRequest request, StreamObserver<RangeResponse> responseObserver) {

        service.rangeStream(request.getFrom(), request.getTo(), tuple -> {

            RangeResponse.Builder resp = RangeResponse.newBuilder()
                    .setKey((String) tuple[0]);

            if (tuple[1] != null) {
                resp.setValue(com.google.protobuf.ByteString.copyFrom((byte[]) tuple[1]));
                resp.setIsNull(false);
            } else {
                resp.setIsNull(true);
            }

            responseObserver.onNext(resp.build());
        });

        responseObserver.onCompleted();
    }

    @Override
    public void count(CountRequest request, StreamObserver<CountResponse> responseObserver) {
        responseObserver.onNext(CountResponse.newBuilder()
                .setCount(service.count())
                .build());
        responseObserver.onCompleted();
    }
}