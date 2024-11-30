package dslab.assignment2.broker;

import dslab.ComponentFactory;
import dslab.broker.IBroker;
import dslab.config.BrokerConfig;
import dslab.util.ConfigFactory;
import dslab.util.Constants;
import dslab.util.Util;
import dslab.util.helper.TelnetClientHelper;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class BaseSingleBrokerTest {

    protected static final BrokerConfig config = ConfigFactory.createBrokerConfigA2();

    protected IBroker broker;
    protected Thread brokerThread;

    protected abstract void initTelnetClientHelpers() throws IOException;

    protected abstract void closeTelnetClientHelpers() throws IOException;

    /**
     * Sets up the broker and TelnetClientHelpers
     * Starts the broker in a background thread and ensures it is ready for connections.
     *
     * @throws IOException if there is an error during setup
     */
    @Timeout(value = 1500, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    @BeforeEach
    void beforeEach() throws IOException {
        log.debug("Execute beforeEach() - Creating the broker");
        broker = ComponentFactory.createBroker(config);

        // Run the broker in a background thread since it blocks the test process
        brokerThread = new Thread(broker);
        brokerThread.start();

        // Probe if the socket is up and ready for commands
        TelnetClientHelper waitForConnHelper = new TelnetClientHelper(Constants.LOCALHOST, config.brokerPort());

        // If this helper connects successfully, then the Broker is ready to accept further connections
        waitForConnHelper.waitForInitConnection();
        waitForConnHelper.disconnect();

        log.debug("Broker parent thread started");

        initTelnetClientHelpers();
    }

    /**
     * Cleans up resources (publisher, subscriber) and shuts down the broker after each test.
     */
    @AfterEach
    @Timeout(value = 1500, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void afterEach() {
        try {
            closeTelnetClientHelpers();
        } catch (IOException e) {
            log.debug("afterEach(): unable to close TCP Helpers", e);
        }

        log.debug("Execute afterEach() - Stopping the broker.");
        if (broker != null) {
            log.debug("Invoking broker.shutdown()");
            broker.shutdown();
        }

        try {
            // Wait for the broker thread to shut down
            if (brokerThread != null && brokerThread.isAlive()) {
                log.debug("Broker thread still alive.");
                brokerThread.join();
                log.debug("Broker thread has finished.");
            }
        } catch (InterruptedException e) {
            log.warn("Broker thread interrupted.");
            Thread.currentThread().interrupt();
        }

        log.debug("Waiting for connections on TCP port {} to close", config.brokerPort());
        Util.waitForTcpPortsToClose(config.brokerPort());
        log.debug("All TCP connections on port {} are closed.", config.brokerPort());
    }
}
