package dslab.assignment2.broker;

import dslab.util.Constants;
import dslab.util.Global;
import dslab.util.helper.TelnetClientHelper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static dslab.util.CommandBuilder.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MultipleExchangeScenarioTest extends BaseSingleBrokerTest {

    private static final String queue1Name = String.format("queue-%s", Global.SECURE_STRING_GENERATOR.getSecureString());
    private static final String queue2Name = String.format("queue-%s", Global.SECURE_STRING_GENERATOR.getSecureString());
    private TelnetClientHelper pub1;
    private TelnetClientHelper pub2;
    private TelnetClientHelper sub1;
    private TelnetClientHelper sub2;

    @Override
    protected void initTelnetClientHelpers() throws IOException {
        pub1 = new TelnetClientHelper(Constants.LOCALHOST, config.brokerPort());
        pub2 = new TelnetClientHelper(Constants.LOCALHOST, config.brokerPort());
        sub1 = new TelnetClientHelper(Constants.LOCALHOST, config.brokerPort());
        sub2 = new TelnetClientHelper(Constants.LOCALHOST, config.brokerPort());

        sub1.connectAndReadResponse();
        sub2.connectAndReadResponse();
        pub1.connectAndReadResponse();
        pub2.connectAndReadResponse();
    }

    @Override
    protected void closeTelnetClientHelpers() throws IOException {
        pub1.disconnect();
        pub2.disconnect();
        sub1.disconnect();
        sub2.disconnect();
    }

    /**
     * Tests the scenario with two publishers and two subscribers interacting with 2 exchange types.
     * Setup (2 Exchanges):
     * P1 --> Fanout --> Queue 1
     * P2 --> Direct --key--> Queue 2
     *
     * @throws IOException IOException if there is an error connecting or sending commands
     */
    @Test
    @Timeout(value = 1500, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void publish_to_fanout_exchange_and_direct_exchange_successfully() throws IOException {
        final String key = "key-%s".formatted(Global.SECURE_STRING_GENERATOR.getSecureString());

        sub1.subscribe("fanout-1", "fanout", queue1Name, "none");
        sub2.subscribe("direct-1", "direct", queue2Name, key);

        pub1.sendCommandAndReadResponse(exchange("fanout", "fanout-1"));
        pub2.sendCommandAndReadResponse(exchange("direct", "direct-1"));

        pub1.publish("none", "VERIFICATION-MESSAGE-FANOUT");
        assertEquals("VERIFICATION-MESSAGE-FANOUT", sub1.readResponse());

        pub2.publish(key, "VERIFICATION-MESSAGE-DIRECT");
        assertEquals("VERIFICATION-MESSAGE-DIRECT", sub2.readResponse());
    }

    /**
     * Tests a scenario with three exchanges (default, direct, topic) and two queues.
     * <p>
     * - Publisher 1 sends messages to:
     * - Queue 1 using the direct exchange with routing key keyOne
     * - Queue 2 using the direct exchange with routing key keyTwo
     * <p>
     * - Publisher 2 sends messages to:
     * - Queue 1 using the topic exchange with the pattern "topic.*"
     * <p>
     * - Publisher 1 also uses the default exchange to send messages to Queue 1 and Queue 2.
     * <p>
     * This test verifies that messages are correctly routed to the appropriate queues for each exchange type.
     * <p>
     * Setup:
     * P1 --> Direct --keyOne--> Queue 1
     * P1 --> Direct --keyTwo--> Queue 2
     * <p>
     * P2 --> Topic --"topic.*"--> Queue 1
     * <p>
     * P1 --> Default --"Queue 1"--> Queue 1
     * P1 --> Default --"Queue 2"--> Queue 2
     *
     * @throws IOException if there is an error connecting or sending commands
     */
    @Test
    @Timeout(value = 1500, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void three_exchanges_two_queues_successfully() throws IOException {
        final String key1 = "key-%s".formatted(Global.SECURE_STRING_GENERATOR.getSecureString());
        final String key2 = "key-%s".formatted(Global.SECURE_STRING_GENERATOR.getSecureString());
        final String topicBase = "topic-%s".formatted(Global.SECURE_STRING_GENERATOR.getSecureString());

        // Setup Topic Exchange, bind Queue 1 to it, subscribe to Queue 1
        sub1.subscribe("topic-1", "topic", queue1Name, String.format("%s.*", topicBase));

        // Setup Direct Exchange, Create Queue 2, Bind Queue1 and Queue 2, Subscribe to Queue2
        sub2.sendCommandAndReadResponse(exchange("direct", "direct-1"));
        sub2.sendCommandAndReadResponse(queue(queue1Name));
        sub2.sendCommandAndReadResponse(bind(key1));
        sub2.sendCommandAndReadResponse(queue(queue2Name));
        sub2.sendCommandAndReadResponse(bind(key2));
        sub2.sendCommandAndReadResponse(subscribe());

        // Set target exchanges for publishers
        pub1.sendCommandAndReadResponse(exchange("direct", "direct-1"));
        pub2.sendCommandAndReadResponse(exchange("topic", "topic-1"));

        // P1 --> Direct --key1--> Queue 1 (Sub 1)
        pub1.publish(key1, "VERIFICATION-MESSAGE-DIRECT-QUEUE-1");
        assertEquals("VERIFICATION-MESSAGE-DIRECT-QUEUE-1", sub1.readResponse());
        // P2 --> Topic --"topic.at"--> Queue 1 (Sub 1)
        pub2.publish(String.format("%s.at", topicBase), "VERIFICATION-MESSAGE-TOPIC-QUEUE-1");
        assertEquals("VERIFICATION-MESSAGE-TOPIC-QUEUE-1", sub1.readResponse());
        // P1 --> Direct --key2--> Queue 2 (Sub 2)
        pub1.publish(key2, "VERIFICATION-MESSAGE-DIRECT-QUEUE-2");
        assertEquals("VERIFICATION-MESSAGE-DIRECT-QUEUE-2", sub2.readResponse());

        // Change the target exchange for publisher 1 to default exchange
        pub1.sendCommandAndReadResponse(exchange("default", "default"));

        // P1 --> Default --"Queue 1"--> Queue 1 (Sub 1)
        pub1.publish(queue1Name, "VERIFICATION-MESSAGE-DEFAULT-QUEUE-1");
        assertEquals("VERIFICATION-MESSAGE-DEFAULT-QUEUE-1", sub1.readResponse());
        // P1 --> Default --"Queue 2"--> Queue 2 (Sub 2)
        pub1.publish(queue2Name, "VERIFICATION-MESSAGE-DEFAULT-QUEUE-2");
        assertEquals("VERIFICATION-MESSAGE-DEFAULT-QUEUE-2", sub2.readResponse());
    }
}
