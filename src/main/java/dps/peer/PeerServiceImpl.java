package dps.peer;

import dps.common.model.ProductionLine;
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

            // Add the new peer to local thread-safe topology registry
            node.addPeer(newPeer);

            System.out.println("Node " + node.getSelf().id() + ": accepted presentation from new peer: "
                    + newPeer.id() + " (" + newPeer.ip() + ":" + newPeer.port() + ")");

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
}
