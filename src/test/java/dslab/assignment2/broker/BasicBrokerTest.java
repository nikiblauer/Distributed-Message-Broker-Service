package dslab.assignment2.broker;

import dslab.util.Constants;
import dslab.util.helper.TelnetClientHelper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BasicBrokerTest extends BaseSingleBrokerTest {

    @Override
    protected void initTelnetClientHelpers() throws IOException {
        // Not needed
    }

    @Override
    protected void closeTelnetClientHelpers() throws IOException {
        // Not needed
    }

    /**
     * Verifies that the server shuts down correctly and no longer accepts connections afterwards
     */
    @Test
    @Timeout(value = 2000, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void broker_shutdown_successfully() {
        assertDoesNotThrow(() -> broker.shutdown(), "An unhandled exception occurred during the shutdown of the broker.");

        // Use Awaitility to wait until the broker thread is no longer alive
        await().atMost(2, TimeUnit.SECONDS).pollInterval(5, TimeUnit.MILLISECONDS).untilAsserted(() -> assertThat(brokerThread.isAlive()).isFalse());

        // TCP client to attempt connection after shutdown
        TelnetClientHelper helper = new TelnetClientHelper(Constants.LOCALHOST, config.brokerPort());

        // Use Awaitility to ensure that the connection fails, as the broker should no longer accept connections
        await().atMost(1, TimeUnit.SECONDS).pollInterval(5, TimeUnit.MILLISECONDS).untilAsserted(() -> assertThrows(IOException.class, () -> {
            helper.connectAndReadResponse();
            helper.disconnect();
        }));
    }
}
