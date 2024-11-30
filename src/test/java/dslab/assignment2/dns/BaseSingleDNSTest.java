package dslab.assignment2.dns;

import dslab.ComponentFactory;
import dslab.config.DNSServerConfig;
import dslab.dns.IDNSServer;
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
public abstract class BaseSingleDNSTest {

    protected static final DNSServerConfig config = ConfigFactory.createDNSServerConfig();

    protected IDNSServer dnsServer;
    protected Thread dnsThread;

    protected abstract void initTelnetClientHelpers() throws IOException;

    protected abstract void closeTelnetClientHelpers() throws IOException;

    @Timeout(value = 1500, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    @BeforeEach
    public void beforeEach() throws IOException {
        log.debug("Execute beforeEach() - Creating the dns server");
        dnsServer = ComponentFactory.createDNSServer(config);

        // Run the dns server in a background thread since it blocks the test process
        dnsThread = new Thread(dnsServer);
        dnsThread.start();

        // Probe if the socket is up and ready for commands
        TelnetClientHelper waitForConnHelper = new TelnetClientHelper(Constants.LOCALHOST, config.port());

        // If this helper connects successfully, then the dns server is ready to accept further connections
        waitForConnHelper.waitForInitConnection();
        waitForConnHelper.disconnect();
        log.debug("dns server parent thread started");

        initTelnetClientHelpers();
    }

    @AfterEach
    @Timeout(value = 1500, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    public void afterEach() {
        try {
            closeTelnetClientHelpers();
        } catch (IOException e) {
            log.debug("afterEach(): Unable to close TCP helper", e);
        }
        log.debug("Execute afterEach() - Stopping the dns server.");
        if (dnsServer != null) {
            log.debug("Invoking dns server.shutdown()");
            dnsServer.shutdown();
        }

        try {
            // Wait for the dns server thread to shut down
            if (dnsThread != null && dnsThread.isAlive()) {
                log.debug("dns server thread still alive.");
                dnsThread.join();
                log.debug("dns server thread has finished.");
            }
        } catch (InterruptedException e) {
            log.warn("dns server thread interrupted.");
            Thread.currentThread().interrupt();
        }

        log.debug("Waiting for connections on TCP port {} to close", config.port());
        Util.waitForTcpPortsToClose(config.port());
        log.debug("All TCP connections on port {} are closed.", config.port());
    }
}
