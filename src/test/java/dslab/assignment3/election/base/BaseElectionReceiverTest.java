package dslab.assignment3.election.base;

import dslab.ComponentFactory;
import dslab.broker.IBroker;
import dslab.config.BrokerConfig;
import dslab.util.*;
import dslab.util.helper.TelnetClientHelper;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Base class for integration tests of the broker election protocol with multiple receivers.
 *
 * <p>This abstract test class is used to set up a broker and multiple receiver nodes for testing election
 * protocol scenarios in a distributed system. Concrete subclasses should implement specific tests for
 * different election behaviors.</p>
 */
@Slf4j
public abstract class BaseElectionReceiverTest implements BaseElectionTest {

    protected static final int BROKER_ELECTION_ID = 100;

    protected int numOfReceivers = getNumOfReceivers();

    protected final int[] receiverPorts = createReceiverPorts(numOfReceivers);
    protected final int[] receiverIds = createReceiverIDs(numOfReceivers);

    protected final BrokerConfig config = ConfigFactory.createBrokerConfigA3(getElectionType(), BROKER_ELECTION_ID, receiverPorts, receiverIds);

    protected IBroker broker;
    protected Thread brokerThread;

    // Helper and receiver to mimic adjacent Nodes in the ring
    protected MockServer[] receivers;
    protected Thread[] receiverThreads;
    protected TelnetClientHelper sender;

    // Points to receivers[0], used for simpler testing
    protected MockServer receiver;

    /**
     * Abstract method to be implemented by subclasses to provide the number of receivers to be created.
     *
     * @return the number of receivers.
     */
    protected abstract int getNumOfReceivers();

    @Timeout(value = 1500, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    @BeforeEach
    public void beforeEach() throws IOException {
        log.debug("Execute beforeEach() - Starting the broker");
        broker = ComponentFactory.createBroker(config);
        brokerThread = new Thread(broker);
        brokerThread.start();

        log.debug("Execute beforeEach() - Starting the receivers");
        receivers = new MockServer[numOfReceivers];
        receiverThreads = new Thread[numOfReceivers];
        for (int i = 0; i < numOfReceivers; i++) {
            receivers[i] = new MockServer(receiverPorts[i]);
            receiverThreads[i] = new Thread(receivers[i]);
            receiverThreads[i].start();
        }

        // Set a reference to the first receiver to make testing easier
        receiver = receivers[0];

        // Probe if the socket is up and ready for commands
        // If this helper connects successfully, then the broker is ready to accept further connections
        TelnetClientHelper waitForConnHelper = new TelnetClientHelper(Constants.LOCALHOST, config.electionPort());
        waitForConnHelper.waitForInitConnection();
        waitForConnHelper.disconnect();
        log.debug("broker parent thread started");

        // If this helper connects successfully, then the mock server is ready to accept further connections
        for (int i = 0; i < numOfReceivers; i++) {
            waitForConnHelper = new TelnetClientHelper(Constants.LOCALHOST, receiverPorts[i]);
            waitForConnHelper.waitForInitConnection();
            waitForConnHelper.disconnect();
        }
        log.debug("receiver parent threads started");

        // Create helper and receiver to mimic adjacent Nodes in the ring
        sender = new TelnetClientHelper(Constants.LOCALHOST, config.electionPort());
    }

    @Timeout(value = 1500, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    @AfterEach
    public void afterEach() {
        receiver = null; // clear reference to receiver-0

        log.debug("Execute afterEach() - Stopping the broker.");
        if (broker != null) {
            log.debug("Invoking broker-0.shutdown()");
            broker.shutdown();
        }

        log.debug("Execute afterEach() - Stopping the receivers.");
        for (int i = 0; i < receivers.length; i++) {
            if (receivers[i] != null) {
                log.debug("Invoking receiver-{}.shutdown()", i);
                receivers[i].shutdown();
            }
        }

        try {
            log.debug("Waiting for broker to shut down");
            if (brokerThread != null && brokerThread.isAlive()) {
                log.debug("broker thread  still alive.");
                brokerThread.join();
                log.debug("broker thread has finished.");
            }

            log.debug("Waiting for receivers to shut down");
            for (Thread receiverThread : receiverThreads) {
                if (receiverThread != null && receiverThread.isAlive()) {
                    log.debug("receiver thread {} still alive.", receiverThread.getName());
                    receiverThread.join();
                    log.debug("receiver thread {} has finished.", receiverThread.getName());
                }
            }

        } catch (InterruptedException e) {
            log.warn("broker thread interrupted.");
            Thread.currentThread().interrupt();
        }

        log.debug("Waiting for connections on TCP ports to close");
        Util.waitForTcpPortsToClose(config.port());
        Util.waitForTcpPortsToClose(receiverPorts);
        log.debug("All TCP Ports are closed");
    }

    /**
     * Creates an array of ports for the receivers based on the number of receivers.
     *
     * @param numOfReceivers the number of receiver nodes.
     * @return an array of port numbers for the receivers.
     */
    protected int[] createReceiverPorts(int numOfReceivers) {
        int[] ports = new int[numOfReceivers];
        for (int i = 0; i < numOfReceivers; i++) {
            ports[i] = Constants.BROKER_BASE_PORT + 11 + 10 * i;
        }

        return ports;
    }

    /**
     * Creates an array of unique IDs for the receivers based on the number of receivers.
     *
     * @param numOfReceivers the number of receiver nodes.
     * @return an array of unique IDs for the receivers.
     */
    protected int[] createReceiverIDs(int numOfReceivers) {
        int[] ids = new int[numOfReceivers];
        for (int i = 0; i < numOfReceivers; i++) {
            ids[i] = BROKER_ELECTION_ID + i + 1;
        }

        return ids;
    }

}
