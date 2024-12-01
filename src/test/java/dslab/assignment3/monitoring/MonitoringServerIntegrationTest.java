package dslab.assignment3.monitoring;

import dslab.ComponentFactory;
import dslab.broker.IBroker;
import dslab.config.BrokerConfig;
import dslab.config.MonitoringServerConfig;
import dslab.monitoring.IMonitoringServer;
import dslab.util.ConfigFactory;
import dslab.util.Constants;
import dslab.util.Util;
import dslab.util.grading.LocalGradingExtension;
import dslab.util.grading.annotations.GitHubClassroomGrading;
import dslab.util.helper.TelnetClientHelper;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static dslab.util.CommandBuilder.bind;
import static dslab.util.CommandBuilder.exchange;
import static dslab.util.CommandBuilder.publish;
import static dslab.util.CommandBuilder.queue;
import static dslab.util.monitoring.MonitoringUtil.waitForMonitoringServerToUpdateDatabase;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration tests for the {@link IMonitoringServer} and {@link IBroker} components.
 * This test class verifies the behavior of brokers and the monitoring server in a distributed environment.
 *
 * <p>The tests involve creating multiple broker instances and a monitoring server, initializing
 * them, and ensuring that the brokers send monitoring data correctly upon receiving messages.
 * The Awaitility framework is used to handle asynchronous behavior and waiting conditions.</p>
 */
@Slf4j
@ExtendWith(LocalGradingExtension.class)
public class MonitoringServerIntegrationTest {

    private static final int NUM_BROKERS = 2;

    private final BrokerConfig[] brokerConfigs = ConfigFactory.createBrokerConfigsA3(NUM_BROKERS, "none", new Integer[]{0, 1, 2});
    private final MonitoringServerConfig monitoringServerConfig = ConfigFactory.createMonitoringServerConfig();
    private final IBroker[] brokers = new IBroker[NUM_BROKERS];
    private final Thread[] brokerThreads = new Thread[NUM_BROKERS];

    private IMonitoringServer monitoringServer;
    private Thread monitoringServerThread;

    @Timeout(value = 1500, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    @BeforeEach
    public void beforeEach() throws IOException {
        log.debug("Execute beforeEach() - Creating the monitoring server");
        monitoringServer = ComponentFactory.createMonitoringServer(monitoringServerConfig);

        // Run the monitoring server in a background thread since it blocks the test process
        monitoringServerThread = new Thread(monitoringServer);
        monitoringServerThread.start();

        log.debug("Execute beforeEach() - Creating the brokers");
        for (int i = 0; i < NUM_BROKERS; i++) {
            brokers[i] = ComponentFactory.createBroker(brokerConfigs[i]);
            brokerThreads[i] = new Thread(brokers[i]);
            brokerThreads[i].start();
        }

        await()
                .atMost(1, TimeUnit.SECONDS)                // Maximum wait time (adjust as needed)
                .pollInterval(10, TimeUnit.MILLISECONDS)   // Poll every 50 milliseconds
                .until(() -> Util.isUdpPortListening(Constants.LOCALHOST, monitoringServerConfig.monitoringPort()));
        log.debug("monitoring server parent thread started");

        // Probe if the socket is up and ready for commands
        // If this helper connects successfully, then the broker is ready to accept further connections
        for (int i = 0; i < NUM_BROKERS; i++) {
            TelnetClientHelper waitForBrokerConnHelper = new TelnetClientHelper(Constants.LOCALHOST, brokerConfigs[i].port());
            waitForBrokerConnHelper.waitForInitConnection();
            waitForBrokerConnHelper.disconnect();
            log.debug("broker parent thread {} started", i);
        }
    }

    @AfterEach
    @Timeout(value = 1500, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    public void afterEach() {
        log.debug("Execute afterEach() - Stopping the monitoring server");
        if (monitoringServer != null) {
            log.debug("Invoking monitoring server.shutdown()");
            monitoringServer.shutdown();
        }

        log.debug("Execute afterEach() - Stopping the brokers.");
        for (int i = 0; i < NUM_BROKERS; i++) {
            if (brokers[0] != null) {
                log.debug("Invoking broker-{}.shutdown()", i);
                brokers[i].shutdown();
            }
        }

        try {
            log.debug("Waiting for monitoring server to shut down");
            if (monitoringServerThread != null && monitoringServerThread.isAlive()) {
                log.debug("monitoring server thread still alive.");
                monitoringServerThread.join();
                log.debug("monitoring server thread has finished.");
            }

            log.debug("Waiting for broker to shut down");
            for (int i = 0; i < NUM_BROKERS; i++) {
                if (brokerThreads[i] != null && brokerThreads[i].isAlive()) {
                    log.debug("broker thread {} still alive.", i);
                    brokerThreads[i].join();
                    log.debug("broker thread {} has finished.", i);
                }
            }
        } catch (InterruptedException e) {
            log.warn("monitoring or broker thread interrupted.");
            Thread.currentThread().interrupt();
        }

        for (BrokerConfig config : brokerConfigs) {
            log.debug("Waiting for connections on TCP ports {}, {} to close", config.port(), config.electionPort());
            Util.waitForTcpPortsToClose(config.port(), config.electionPort());
            log.debug("TCP Sockets on ports {}, {} are closed", config.port(), config.electionPort());
        }
    }

    @GitHubClassroomGrading(maxScore = 10)
    @Test
    @Timeout(value = 2000, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void monitoring_messageBrokerSendsMonitoringDataUponReceivingMessage() throws IOException {
        final int numberOfMessages = 4;

        // Setup exchange queues for broker 0
        TelnetClientHelper helper = new TelnetClientHelper(Constants.LOCALHOST, brokerConfigs[0].port());
        helper.connectAndReadResponse();
        helper.sendCommandAndReadResponse(exchange("direct", "direct-0"));
        helper.sendCommandAndReadResponse(queue("queue-0"));
        helper.sendCommandAndReadResponse(bind("key.zero"));
        helper.sendCommandAndReadResponse(queue("queue-1"));
        helper.sendCommandAndReadResponse(bind("key.one"));

        helper.sendCommandAndReadResponse(publish("key.zero", "irrelevant-msg"));
        helper.sendCommandAndReadResponse(publish("key.zero", "irrelevant-msg"));
        helper.sendCommandAndReadResponse(publish("key.one", "irrelevant-msg"));
        helper.disconnect();

        // Setup exchange queues for broker 1
        helper = new TelnetClientHelper(Constants.LOCALHOST, brokerConfigs[1].port());
        helper.connectAndReadResponse();
        helper.sendCommandAndReadResponse(exchange("direct", "direct-0"));
        helper.sendCommandAndReadResponse(queue("queue-0"));
        helper.sendCommandAndReadResponse(bind("key.special"));
        helper.sendCommandAndReadResponse(publish("key.special", "irrelevant-msg"));
        helper.disconnect();

        waitForMonitoringServerToUpdateDatabase(monitoringServer, numberOfMessages);

        assertEquals(numberOfMessages, monitoringServer.receivedMessages());
        assertThat(monitoringServer.getStatistics()).contains(
                String.format("%s:%d", brokerConfigs[0].host(), brokerConfigs[0].port()),
                "key.zero 2", "key.one 1",
                String.format("%s:%d", brokerConfigs[1].host(), brokerConfigs[1].port()),
                "key.special 1");
    }
}
