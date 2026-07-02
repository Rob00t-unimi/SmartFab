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

    /**
     * Accept the presentation request of another node and add this node into local topology of "this.node".
     **/
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

            System.out.println("[PEER " + node.getSelf().id() + "] Received gRPC presentation request from Node " 
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


    /**
     * Handles incoming calibration requests from other peers (Ricart-Agrawala Server-side logic).
     * 
     * Updates the local Lamport logical clock using the received timestamp:
     * Clock = max(localClock, receivedTimestamp) + 1.
     * 
     * Compares the priorities to decide whether to reply immediately or defer the reply:
     * - Defers if local state is UNDER_CALIBRATION.
     * - Defers if local state is WAITING_FOR_CALIBRATION and the local node has higher priority.
     *   Priority is determined by:
     *     1) Criticality (higher criticality has priority).
     *     2) Node ID (higher ID has priority as a tie-breaker).
     * - Replies immediately with CalibrationReply otherwise.
     */
    @Override
    public void requestCalibration(CalibrationRequest request, StreamObserver<CalibrationReply> responseObserver) {
        try {
            int senderId = request.getSenderId();
            double senderCriticality = request.getCriticality();
            long senderTimestamp = request.getTimestamp();

            // Update our local Lamport clock upon receiving the message
            node.updateClockOnReceive(senderTimestamp);

            boolean defer = false;
            String decisionReason = "";

            synchronized (node) {
                OperationalState localState = node.getState();  // get current local state
                int localId = node.getSelf().id();              // get local id

                if (localState == OperationalState.UNDER_CALIBRATION) {
                    // We are currently in calibration, so we have the resource. Defer the reply.
                    defer = true;
                    decisionReason = "local node is already UNDER_CALIBRATION";
                } else if (localState == OperationalState.WAITING_FOR_CALIBRATION) {
                    // Both want the resource. Compare priority (criticality then ID)
                    double localCriticality = node.calcCriticality();

                    if (localCriticality > senderCriticality) {
                        defer = true;
                        decisionReason = String.format("local WAITING with higher criticality (local: %.4f > sender: %.4f)", 
                                localCriticality, senderCriticality);
                    } else if (localCriticality == senderCriticality) {
                        if (localId > senderId) {
                            defer = true;
                            decisionReason = String.format("local WAITING with equal criticality (%.4f) but higher ID (local: %d > sender: %d) [tie-breaker]", 
                                    localCriticality, localId, senderId);
                        } else {
                            decisionReason = String.format("local WAITING with equal criticality (%.4f) but lower ID (local: %d < sender: %d) [tie-breaker]", 
                                    localCriticality, localId, senderId);
                        }
                    } else {
                        decisionReason = String.format("local WAITING with lower criticality (local: %.4f < sender: %.4f)", 
                                localCriticality, senderCriticality);
                    }
                } else {
                    decisionReason = "local node is FULLY_OPERATIONAL";
                }
            }

            if (defer) {
                // Store the observer in the deferred list
                node.addDeferredObserver(senderId, responseObserver, decisionReason);
            } else {
                // Reply immediately
                System.out.println("[PEER " + node.getSelf().id() + "] Replying IMMEDIATELY to calibration request from Node " 
                        + senderId + " (Clock: " + senderTimestamp + ", Criticality: " + String.format("%.4f", senderCriticality) 
                        + ") - Reason: " + decisionReason);

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
