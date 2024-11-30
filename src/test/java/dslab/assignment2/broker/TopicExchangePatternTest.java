package dslab.assignment2.broker;

import dslab.util.Constants;
import dslab.util.Global;
import dslab.util.helper.TelnetClientHelper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static dslab.util.CommandBuilder.exchange;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TopicExchangePatternTest extends BaseSingleBrokerTest {

    private TelnetClientHelper publisher;
    private TelnetClientHelper subscriber;

    private final String exchangeName = String.format("exchange-%s", Global.SECURE_STRING_GENERATOR.getSecureString());
    private final String queueName = String.format("queue-%s", Global.SECURE_STRING_GENERATOR.getSecureString());
    private final String queue2Name = String.format("queue-%s", Global.SECURE_STRING_GENERATOR.getSecureString());
    private final String keyBase = "key-%s".formatted(Global.SECURE_STRING_GENERATOR.getSecureString());

    @Override
    protected void initTelnetClientHelpers() throws IOException {
        publisher = new TelnetClientHelper(Constants.LOCALHOST, config.brokerPort());
        subscriber = new TelnetClientHelper(Constants.LOCALHOST, config.brokerPort());

        subscriber.connectAndReadResponse();
        publisher.connectAndReadResponse();
    }

    @Override
    protected void closeTelnetClientHelpers() throws IOException {
        publisher.disconnect();
        subscriber.disconnect();
    }

    @Test
    @Timeout(value = 3500, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void star_pattern_tests() throws IOException {
        // beforeEach() called by junit
        single_star_successfully();
        afterEach();

        beforeEach();
        single_star_unsuccessfully();
        afterEach();

        beforeEach();
        two_stars_edge_successfully();
        afterEach();

        beforeEach();
        two_stars_in_between_successfully();
        afterEach();

        beforeEach();
        two_stars_edge_unsuccessfully();
        // afterEach() called by junit
    }

    @Test
    @Timeout(value = 2500, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void hashtag_pattern_tests() throws IOException {
        // beforeEach() called by JUnit
        single_hashtag_successfully();
        afterEach();

        beforeEach();
        single_hashtag_unsuccessfully();
        afterEach();

        beforeEach();
        two_hashtags_successfully();
        // afterEach() called by JUnit
    }

    @Test
    @Timeout(value = 2500, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void basic_logic_tests() throws IOException {
        // beforeEach() called by JUnit
        basic_key_no_placeholders_successfully();
        afterEach();

        beforeEach();
        two_matching_keys_successfully();
        // afterEach() called by JUnit
    }

    /**
     * Tests the topic exchange pattern where the binding key does not contain any placeholders.
     * The message is expected to be routed to the correct subscriber.
     *
     * @throws IOException if there is an error in communication
     */
    void basic_key_no_placeholders_successfully() throws IOException {
        subscriber.subscribe(exchangeName, "topic", queueName, String.format("%s.at", keyBase));
        publisher.publish(exchangeName, "topic", String.format("%s.at", keyBase), "VERIFICATION-MESSAGE");
        assertEquals("VERIFICATION-MESSAGE", subscriber.readResponse());
    }

    /**
     * Tests a topic exchange pattern where the binding key contains a single '*' placeholder.
     * The message is expected to match and be routed to the correct subscriber.
     *
     * @throws IOException if there is an error in communication
     */
    void single_star_successfully() throws IOException {
        subscriber.subscribe(exchangeName, "topic", queueName, "company.*");

        publisher.publish(exchangeName, "topic", "company.de", "VERIFICATION-MESSAGE");

        assertEquals("VERIFICATION-MESSAGE", subscriber.readResponse());
    }

    /**
     * Tests the case where messages should not be routed to a subscriber due to incorrect
     * key matching with the '*' placeholder in the binding key. Only valid messages will be routed.
     *
     * @throws IOException if there is an error in communication
     */
    void single_star_unsuccessfully() throws IOException {
        subscriber.subscribe(exchangeName, "topic", queueName, String.format("%s.*", keyBase));

        publisher.sendCommandAndReadResponse(exchange("topic", exchangeName));

        publisher.publish(String.format("%s.com.org", keyBase), "DOES-NOT-ARRIVE-1");
        publisher.publish(String.format("www.%s", keyBase), "DOES-NOT-ARRIVE-2");
        publisher.publish(String.format("www.%s.com", keyBase), "DOES-NOT-ARRIVE-3");
        publisher.publish(keyBase, "DOES-NOT-ARRIVE-4");

        publisher.publish(String.format("%s.com", keyBase), "VERIFICATION-MESSAGE");

        assertEquals("VERIFICATION-MESSAGE", subscriber.readResponse());
    }

    /**
     * Tests the case where the binding key contains two '*' placeholders and successfully
     * matches messages that follow the pattern.
     *
     * @throws IOException if there is an error in communication
     */
    void two_stars_edge_successfully() throws IOException {
        subscriber.subscribe(exchangeName, "topic", queueName, String.format("*.%s.*", keyBase));

        publisher.sendCommandAndReadResponse(exchange("topic", exchangeName));

        publisher.publish(String.format("www.%s.com", keyBase), "VERIFICATION-MESSAGE-1");
        assertEquals("VERIFICATION-MESSAGE-1", subscriber.readResponse());
        publisher.publish(String.format("12345abcd.%s.de", keyBase), "VERIFICATION-MESSAGE-2");
        assertEquals("VERIFICATION-MESSAGE-2", subscriber.readResponse());
    }

    /**
     * Tests the case where two '*' placeholders are used in between words of the binding key.
     * Messages following this pattern will be routed successfully.
     *
     * @throws IOException if there is an error in communication
     */
    void two_stars_in_between_successfully() throws IOException {
        subscriber.subscribe(exchangeName, "topic", queueName, String.format("www.*.%s.*", keyBase));

        publisher.sendCommandAndReadResponse(exchange("topic", exchangeName));

        publisher.publish(String.format("www.at.%s.com", keyBase), "VERIFICATION-MESSAGE-1");
        assertEquals("VERIFICATION-MESSAGE-1", subscriber.readResponse());
        publisher.publish(String.format("www.12345abcd.%s.de", keyBase), "VERIFICATION-MESSAGE-2");
        assertEquals("VERIFICATION-MESSAGE-2", subscriber.readResponse());
    }

    /**
     * Tests the case where the message routing fails due to incorrect usage of the '*' placeholder
     * in the binding key. Only messages matching the correct pattern will be routed.
     *
     * @throws IOException if there is an error in communication
     */
    void two_stars_edge_unsuccessfully() throws IOException {
        subscriber.subscribe(exchangeName, "topic", queueName, String.format("*.%s.*", keyBase));

        publisher.sendCommandAndReadResponse(exchange("topic", exchangeName));

        publisher.publish(String.format("%s.com", keyBase), "THIS-SHOULD-BE-DISCARDED");
        publisher.publish(String.format("www.%s", keyBase), "THIS-SHOULD-BE-DISCARDED");
        publisher.publish(String.format("%s", keyBase), "THIS-SHOULD-BE-DISCARDED");
        publisher.publish(String.format(".%s.", keyBase), "THIS-SHOULD-BE-DISCARDED");
        publisher.publish(String.format("at.%s1.com", keyBase), "THIS-SHOULD-BE-DISCARDED");
        publisher.publish("", "THIS-SHOULD-BE-DISCARDED");

        publisher.publish(String.format("www.%s.com", keyBase), "VERIFICATION-MESSAGE");

        assertEquals("VERIFICATION-MESSAGE", subscriber.readResponse());
    }

    /**
     * Tests the case where the binding key uses a single '#' placeholder, allowing the message
     * to match multiple levels in the routing key. All matching messages are routed.
     *
     * @throws IOException if there is an error in communication
     */
    void single_hashtag_successfully() throws IOException {
        subscriber.subscribe(exchangeName, "topic", queueName, String.format("%s.#", keyBase));

        publisher.sendCommandAndReadResponse(exchange("topic", exchangeName));

        publisher.publish(String.format("%s.com", keyBase), "VERIFICATION-MESSAGE-1");
        assertEquals("VERIFICATION-MESSAGE-1", subscriber.readResponse());
        publisher.publish(String.format("%s.com.de", keyBase), "VERIFICATION-MESSAGE-2");
        assertEquals("VERIFICATION-MESSAGE-2", subscriber.readResponse());
        publisher.publish(String.format("%s.com.de.at.cf.zz", keyBase), "VERIFICATION-MESSAGE-3");
        assertEquals("VERIFICATION-MESSAGE-3", subscriber.readResponse());
        publisher.publish(String.format("%s", keyBase), "VERIFICATION-MESSAGE-4");
        assertEquals("VERIFICATION-MESSAGE-4", subscriber.readResponse());
    }

    /**
     * Tests that messages do not arrive when the binding key uses a single '#' placeholder,
     * but the routing key does not match the pattern.
     *
     * @throws IOException if there is an error in communication
     */
    void single_hashtag_unsuccessfully() throws IOException {
        subscriber.subscribe(exchangeName, "topic", queueName, String.format("%s.#", keyBase));

        publisher.sendCommandAndReadResponse(exchange("topic", exchangeName));

        publisher.publish(String.format("www.%s.com", keyBase), "DOES-NOT-ARRIVE-1");
        publisher.publish(String.format("%s1.com", keyBase), "DOES-NOT-ARRIVE-2");
        publisher.publish(String.format("%scom", keyBase), "DOES-NOT-ARRIVE-3");
        publisher.publish(String.format("%s.com", keyBase), "VERIFICATION-MESSAGE");
        assertEquals("VERIFICATION-MESSAGE", subscriber.readResponse());
    }

    /**
     * Tests the case where the binding key uses two '#' placeholders to allow messages with multiple
     * levels in the routing key. The test ensures that all matching messages are successfully routed.
     *
     * @throws IOException if there is an error in communication
     */
    void two_hashtags_successfully() throws IOException {
        subscriber.subscribe(exchangeName, "topic", queueName, String.format("#.%s.#", keyBase));

        publisher.sendCommandAndReadResponse(exchange("topic", exchangeName));

        publisher.publish(String.format("www.%s.com", keyBase), "VERIFICATION-MESSAGE-1");
        assertEquals("VERIFICATION-MESSAGE-1", subscriber.readResponse());
        publisher.publish(String.format("at.de.%s.com.gov", keyBase), "VERIFICATION-MESSAGE-2");
        assertEquals("VERIFICATION-MESSAGE-2", subscriber.readResponse());
        publisher.publish(String.format("%s.com", keyBase), "VERIFICATION-MESSAGE-3");
        assertEquals("VERIFICATION-MESSAGE-3", subscriber.readResponse());
        publisher.publish(String.format("%s.com.de", keyBase), "VERIFICATION-MESSAGE-4");
        assertEquals("VERIFICATION-MESSAGE-4", subscriber.readResponse());
        publisher.publish(String.format("%s", keyBase), "VERIFICATION-MESSAGE-5");
        assertEquals("VERIFICATION-MESSAGE-5", subscriber.readResponse());
    }

    /**
     * Tests the scenario where two subscribers with different binding keys are subscribed to the
     * same exchange. Both subscribers are expected to receive the message if their keys match.
     *
     * @throws IOException if there is an error in communication
     */
    void two_matching_keys_successfully() throws IOException {
        TelnetClientHelper subscriber2 = new TelnetClientHelper(Constants.LOCALHOST, config.brokerPort());
        subscriber2.connectAndReadResponse();

        subscriber.subscribe(exchangeName, "topic", queueName, String.format("%s.#", keyBase));
        subscriber2.subscribe(exchangeName, "topic", queue2Name, String.format("%s.*", keyBase));

        publisher.publish(exchangeName, "topic", String.format("%s.at", keyBase), "VERIFICATION-MESSAGE");

        assertEquals("VERIFICATION-MESSAGE", subscriber.readResponse());
        assertEquals("VERIFICATION-MESSAGE", subscriber2.readResponse());
    }
}
