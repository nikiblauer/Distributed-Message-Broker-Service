package dslab.assignment3.election.ring;

import dslab.assignment3.election.base.BaseElectionReceiverTest;
import dslab.util.grading.LocalGradingExtension;
import dslab.util.grading.annotations.GitHubClassroomGrading;
import dslab.util.mock.MockServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static dslab.util.CommandBuilder.PING;
import static dslab.util.CommandBuilder.declare;
import static dslab.util.CommandBuilder.elect;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for the Ring Election protocol focusing on multiple receivers.
 *
 * <p>This class extends the base election receiver test class and includes tests to ensure
 * that the ring election protocol correctly sends health notifications to peers after
 * a leader is elected.</p>
 */
@ExtendWith(LocalGradingExtension.class)
public class RingElectionMultipleReceiverTest extends BaseElectionReceiverTest {

    @Override
    protected int getNumOfReceivers() {
        return 2;
    }

    @Override
    public String getElectionType() {
        return "ring";
    }

    @GitHubClassroomGrading(maxScore = 3)
    @Test
    @Timeout(value = 3000, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void ring_becomesLeader_startsSendingHeartbeatsToPeers() throws IOException, InterruptedException {
        // Setup receiver-0 for incoming declare msg
        receiver.expect().expectDeclareReturnAck(BROKER_ELECTION_ID).expectPingReturnPong();

        // Prepare the receivers to expect to the ping from the new leader
        for (MockServer r : receivers) {
            // Compare object pointers using '!=' to exclude receiver-0
            if (r != receiver) r.expect().expectPingReturnPong();
        }

        // become leader by receiving elect <your-own-id>
        sender.connectAndReadResponse();
        sender.sendCommandAndReadResponse(elect(BROKER_ELECTION_ID)); // become leader

        // receiver-0 should receive declare message, as he is the next peer in the ring
        assertEquals(declare(BROKER_ELECTION_ID), receiver.takeFromReceivedCommands());

        // check for health notification
        for (MockServer receiver : receivers) assertEquals(PING, receiver.takeFromReceivedCommands());
    }
}
