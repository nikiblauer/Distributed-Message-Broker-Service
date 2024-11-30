package dslab.assignment3.election.raft;

import dslab.assignment3.election.base.BaseElectionReceiverTest;
import dslab.util.grading.annotations.GitHubClassroomGrading;
import dslab.util.grading.LocalGradingExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.TimeUnit;

import static dslab.util.CommandBuilder.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for the Raft Election Receiver, validating the behavior of election initiation,
 * leader declaration, and health notifications.
 *
 * <p>This class extends the base election receiver test class and includes tests to ensure that
 * the Raft election protocol correctly handles the initiation of elections, votes, and leader
 * health notifications.</p>
 */
@ExtendWith(LocalGradingExtension.class)
public class RaftElectionReceiverTest extends BaseElectionReceiverTest {

    @Override
    protected int getNumOfReceivers() {
        return 2;
    }

    @Override
    public String getElectionType() {
        return "raft";
    }

    @GitHubClassroomGrading(maxScore = 2)
    @Test
    @Timeout(value = 2000, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void raft_initiatesElection_sendsElectMessageToPeers_becomesLeader() throws InterruptedException {
        for (int i = 0; i < numOfReceivers; i++) {
            System.out.println(receivers[i].getQueue());
            receivers[i].setExceptedMessage(elect(BROKER_ELECTION_ID));
            receivers[i].setResponse(vote(i, BROKER_ELECTION_ID));
        }

        broker.initiateElection();

        for (int i = 0; i < numOfReceivers; i++) {
            System.out.println(receivers[i].getQueue());
            assertEquals(elect(BROKER_ELECTION_ID), receivers[i].takeMessage());
        }

        for (int i = 0; i < numOfReceivers; i++) {
            System.out.println(receivers[i].getQueue());
            assertEquals(declare(BROKER_ELECTION_ID), receivers[i].takeMessage());
        }

        assertEquals(BROKER_ELECTION_ID, broker.getLeader());
    }

    @GitHubClassroomGrading(maxScore = 2)
    @Test
    @Timeout(value = 2000, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void raft_initiatesElection_sendsElectMessageToPeers_doesNotBecomeLeader() throws InterruptedException {
        for (int i = 0; i < numOfReceivers; i++) {
            receivers[i].setExceptedMessage(elect(BROKER_ELECTION_ID));
            receivers[i].setResponse(vote(i, BROKER_ELECTION_ID + 1));
        }

        broker.initiateElection();

        for (int i = 0; i < numOfReceivers; i++) {
            assertEquals(elect(BROKER_ELECTION_ID), receivers[i].takeMessage());
        }

       assertThat(broker.getLeader()).isLessThan(0);
    }

    @GitHubClassroomGrading(maxScore = 2)
    @Test
    @Timeout(value = 2000, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void raft_reachesTimeout_initiatesNewElection() throws InterruptedException {
        for (int i = 0; i < numOfReceivers; i++) {
            receivers[i].setExceptedMessage(elect(BROKER_ELECTION_ID));
            receivers[i].setResponse(vote(i, BROKER_ELECTION_ID));
        }

        for (int i = 0; i < numOfReceivers; i++) {
            assertEquals(elect(BROKER_ELECTION_ID), receivers[i].takeMessage());
        }
    }

    @GitHubClassroomGrading(maxScore = 2)
    @Test
    @Timeout(value = 3000, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void raft_becomesLeader_startsSendingHeartbeatsToPeers() throws InterruptedException {
        for (int i = 0; i < numOfReceivers; i++) {
            receivers[i].setExceptedMessage(elect(BROKER_ELECTION_ID));
            receivers[i].setResponse(vote(i, BROKER_ELECTION_ID));
        }

        broker.initiateElection();

        for (int i = 0; i < numOfReceivers; i++) assertEquals(elect(BROKER_ELECTION_ID), receivers[i].takeMessage());

        for (int i = 0; i < numOfReceivers; i++) {
            receivers[i].setExceptedMessage(declare(BROKER_ELECTION_ID));
            receivers[i].setResponse(ack(BROKER_ELECTION_ID - (i + 1)));
        }

        for (int i = 0; i < numOfReceivers; i++) assertEquals(declare(BROKER_ELECTION_ID), receivers[i].takeMessage());

        for (int i = 0; i < numOfReceivers; i++) assertEquals(ping(), receivers[i].takeMessage());
    }
}
