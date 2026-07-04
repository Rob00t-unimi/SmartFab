package dps.peer.net;

import dps.common.model.OperationalState;
import dps.common.model.ProductionLine;
import dps.peer.ProductionLineNode;
import dps.peer.coordinator.RicartAgrawalaCoordinator;
import dps.peer.proto.CalibrationRequest;
import dps.peer.proto.CalibrationReply;
import dps.peer.proto.PeerServiceGrpc;
import dps.peer.proto.PresentationRequest;
import dps.peer.proto.PresentationResponse;
import io.grpc.stub.StreamObserver;

/**
 * gRPC Service (SERVER PEER) implementation for incoming peer requests.
 * Delegates topology additions to NetworkManager and mutual exclusion coordination to RicartAgrawalaCoordinator.
 */
public class GrpcPeerService extends PeerServiceGrpc.PeerServiceImplBase {

    private final ProductionLineNode node;
    private final RicartAgrawalaCoordinator coordinator;

    /**
     * Enumerates the three possible actions when handling a peer's calibration request.
     */
    private enum Action {
        DEFER,             // Defer the reply to the sender peer
        YIELD,             // Reply immediately and execute yielding logic (re-requesting)
        REPLY_IMMEDIATELY  // Reply immediately without yielding
    }
    /**
     * Record pairing the priority action outcome with its detailed execution explanation.
     */
    private record Decision(Action action, String reason) {}

    public GrpcPeerService(ProductionLineNode node) {
        if (node == null) {
            throw new IllegalArgumentException("ProductionLineNode reference cannot be null.");
        }
        this.node = node;
        this.coordinator = node.getCoordinator();
    }

    /** =================================================== PRESENTATION REQUESTS =========================================================================================== **/

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
            node.getNetworkManager().addPeer(newPeer);

            System.out.println("[PEER " + node.getSelf().id() + "] [" + node.getState() + "] Received gRPC presentation request from Node " 
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

    /** =================================================== CALIBRATION REQUESTS =========================================================================================== **/

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
            coordinator.updateClockOnReceive(senderTimestamp);

            Decision decision;
            synchronized (coordinator) {
                decision = evaluatePriority(senderId, senderCriticality);

                if (decision.action() == Action.YIELD) {
                    executeYielding(senderId); // Reply immediately and execute yielding logic (re-requesting)
                }
            }

            if (decision.action() == Action.DEFER) {
                // Store the observer in the deferred list
                coordinator.addDeferredObserver(senderId, responseObserver, decision.reason()); // Defer the reply to the sender peer
            } else {
                sendImmediateReply(senderId, senderTimestamp, senderCriticality, decision.reason(), responseObserver); // Reply immediately
            }

        } catch (Exception e) {
            responseObserver.onError(io.grpc.Status.INTERNAL
                    .withDescription("Internal error during calibration request: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    /**
     * Evaluates calibration priority between local state and incoming request under lock.
     */
    private Decision evaluatePriority(int senderId, double senderCriticality) {
        OperationalState localState = node.getState();
        int localId = node.getSelf().id();

        if (localState == OperationalState.UNDER_CALIBRATION) {
            return new Decision(Action.DEFER, "local node is already UNDER_CALIBRATION");
        } 
        
        if (localState == OperationalState.WAITING_FOR_CALIBRATION) {

            /**
             * When the local node is in the WAITING_FOR_CALIBRATION state
             * (and therefore also wants to calibrate), priorities are compared:
             *
             * - If the Local Node wins -> Action.DEFER
             *   (We have priority: we put the sender on hold and do not reply
             *   until we are finished).
             *
             * - If the Sender Node wins -> Action.YIELD
             *   (The sender has priority: we must reply immediately to let them proceed,
             *   and if they had already replied to us previously, we must invalidate
             *   that consent and queue up again).
             */

            double localCriticality = node.getCoordinator().calcCriticality();

            if (localCriticality > senderCriticality) {
                return new Decision(Action.DEFER, String.format(
                        "local WAITING with higher criticality (local: %.4f > sender: %.4f)", 
                        localCriticality, senderCriticality));
            } 
            
            if (localCriticality == senderCriticality) {
                if (localId > senderId) {
                    return new Decision(Action.DEFER, String.format(
                            "local WAITING with equal criticality (%.4f) but higher ID (local: %d > sender: %d) [tie-breaker]", 
                            localCriticality, localId, senderId));
                } else {
                    return new Decision(Action.YIELD, String.format(
                            "local WAITING with equal criticality (%.4f) but lower ID (local: %d < sender: %d) [tie-breaker]", 
                            localCriticality, localId, senderId));
                }
            } 
            
            return new Decision(Action.YIELD, String.format(
                    "local WAITING with lower criticality (local: %.4f < sender: %.4f)", 
                    localCriticality, senderCriticality));
        }

        return new Decision(Action.REPLY_IMMEDIATELY, "local node is FULLY_OPERATIONAL");
    }

    /**
     * Performs Adapted Ricart-Agrawala yielding: invalidates our received reply and re-requests calibration.
     */
    private void executeYielding(int senderId) {
        if (coordinator.hasReplyFrom(senderId)) {
            /**
             * Yielding logic for Adapted Ricart-Agrawala (Section 4.3.2):
             * Since priority is based on dynamic criticality rather than Lamport clocks, a later request
             * with higher criticality can preempt an earlier request.
             * If we yield to a higher-priority sender, and we already received a reply from them,
             * we must invalidate/remove that reply. We then re-request calibration from them
             * so that they queue/defer us on their end.
             **/
            coordinator.removeReply(senderId);

            // Re-request calibration from this higher-priority peer so that they queue/defer us.
            // This prevents deadlocks when the higher-priority peer is not currently aware that we are waiting.
            ProductionLine senderPeer = node.getNetworkManager().getPeerById(senderId);
            if (senderPeer != null) {
                coordinator.sendCalibrationRequestToPeerAsynchronously(senderPeer);
            }
        }
    }

    /**
     * Sends an immediate CalibrationReply to the requesting peer.
     */
    private void sendImmediateReply(int senderId, long senderTimestamp, double senderCriticality, String reason, StreamObserver<CalibrationReply> responseObserver) {
        System.out.println("[PEER " + node.getSelf().id() + "] [" + node.getState() + "] Replying IMMEDIATELY to calibration request from Node " 
                + senderId + " (Clock: " + senderTimestamp + ", Criticality: " + String.format("%.4f", senderCriticality) 
                + ") - Reason: " + reason);

        CalibrationReply reply = CalibrationReply.getDefaultInstance();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
}
