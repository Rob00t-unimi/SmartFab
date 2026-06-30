package dps.peer;

import dps.common.model.ProductionLine;
import dps.peer.proto.CalibrationRequest;
import dps.peer.proto.CalibrationReply;
import dps.peer.proto.PeerServiceGrpc;
import dps.peer.proto.PresentationRequest;
import dps.peer.proto.PresentationResponse;
import io.grpc.stub.StreamObserver;

public class PeerServiceImpl extends PeerServiceGrpc.PeerServiceImplBase {

    private final ProductionLineNode node;

    public PeerServiceImpl(ProductionLineNode node) {
        if (node == null) {
            throw new IllegalArgumentException("ProductionLineNode reference cannot be null.");
        }
        this.node = node;
    }

    @Override
    public void present(PresentationRequest request, StreamObserver<PresentationResponse> responseObserver) {
        try {
            var senderIdentity = request.getSender();

            // Build the local model representation of the new peer
            ProductionLine newPeer = new ProductionLine(
                    senderIdentity.getId(),
                    senderIdentity.getIp(),
                    senderIdentity.getPort()
            );

            // Add the new peer to our local thread-safe topology registry
            node.addPeer(newPeer);

            System.out.println("[LINEA " + node.getSelf().id() + "] Received gRPC presentation request from Node " 
                    + newPeer.id() + " (" + newPeer.ip() + ":" + newPeer.port() + "). Adding to local topology.");

            // Respond back indicating acceptance
            PresentationResponse response = PresentationResponse.newBuilder()
                    .setAccepted(true)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (IllegalArgumentException e) {
            responseObserver.onError(io.grpc.Status.INVALID_ARGUMENT
                    .withDescription("Invalid identity parameters: " + e.getMessage())
                    .asRuntimeException());
        } catch (Exception e) {
            responseObserver.onError(io.grpc.Status.INTERNAL
                    .withDescription("Internal error during presentation: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void requestCalibration(CalibrationRequest request, StreamObserver<CalibrationReply> responseObserver) {
        try {
            // Skeleton logic for Commit 2: just respond successfully immediately
            CalibrationReply reply = CalibrationReply.getDefaultInstance();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(io.grpc.Status.INTERNAL
                    .withDescription("Internal error during calibration request: " + e.getMessage())
                    .asRuntimeException());
        }
    }
}
