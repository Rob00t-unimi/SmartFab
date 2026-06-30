package dps.peer;

import dps.common.model.OperationalState;
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
            int senderId = request.getSenderId();
            double senderCriticality = request.getCriticality();
            long senderTimestamp = request.getTimestamp();

            // 1. Update our local Lamport clock upon receiving the message
            node.updateClockOnReceive(senderTimestamp);

            boolean defer = false;

            synchronized (node) {
                OperationalState localState = node.getState();
                int localId = node.getSelf().id();

                if (localState == OperationalState.UNDER_CALIBRATION) {
                    // We are currently in calibration, so we have the resource. Defer the reply.
                    defer = true;
                } else if (localState == OperationalState.WAITING_FOR_CALIBRATION) {
                    // Both want the resource. Compare priority (criticality then ID)
                    double localCriticality = (node.getLastCalculatedAverage() - 80.0) / 80.0;

                    if (localCriticality > senderCriticality) {
                        defer = true;
                    } else if (localCriticality == senderCriticality) {
                        if (localId > senderId) {
                            defer = true;
                        }
                    }
                }
            }

            if (defer) {
                // Store the observer in the deferred list
                node.addDeferredObserver(senderId, responseObserver);
            } else {
                // Reply immediately
                System.out.println("[LINEA " + node.getSelf().id() + "] Replying IMMEDIATELY to calibration request from Node " 
                        + senderId + " (Clock: " + senderTimestamp + ", Criticality: " + String.format("%.4f", senderCriticality) + ")");

                CalibrationReply reply = CalibrationReply.getDefaultInstance();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            }

        } catch (Exception e) {
            responseObserver.onError(io.grpc.Status.INTERNAL
                    .withDescription("Internal error during calibration request: " + e.getMessage())
                    .asRuntimeException());
        }
    }
}
