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

/**
 * BasicExchangeTest performs unit tests on the broker's exchange functionality,
 * focusing on multiple connection handling, message publication, and validation
 * of various exchange types (fanout, direct, default). It includes tests that
 * validate correct message delivery and handling of invalid or discarded messages.
 *
 * <p>
 * The following scenarios are covered:
 * <ul>
 *     <li>Testing multiple connections to the broker.</li>
 *     <li>Publishing to fanout, direct, and default exchanges.</li>
 *     <li>Ensuring invalid messages are discarded while valid messages are processed.</li>
 *     <li>Checking queue binding and subscription logic, including delayed subscriptions.</li>
 * </ul>
 * </p>
 */
public class BasicExchangeTest extends BaseSingleBrokerTest {

    private TelnetClientHelper publisher;
    private TelnetClientHelper subscriber;

    private static final String exchangeName = "exchange-%s".formatted(Global.SECURE_STRING_GENERATOR.getSecureString());
    private static final String queueName = "exchange-%s".formatted(Global.SECURE_STRING_GENERATOR.getSecureString());
    private static final String key = "key-%s".formatted(Global.SECURE_STRING_GENERATOR.getSecureString());

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

    /**
     * Verifies that multiple connections can be established to the broker successfully.
     *
     * @throws IOException if there is an error during connection
     */
    @Test
    @Timeout(value = 1500, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void exchange_accepts_multiple_connections_successfully() throws IOException {
        final TelnetClientHelper helper1 = new TelnetClientHelper(Constants.LOCALHOST, config.brokerPort());
        final TelnetClientHelper helper2 = new TelnetClientHelper(Constants.LOCALHOST, config.brokerPort());
        final TelnetClientHelper helper3 = new TelnetClientHelper(Constants.LOCALHOST, config.brokerPort());
        assertEquals("ok SMQP", helper1.connectAndReadResponse());
        assertEquals("ok SMQP", helper2.connectAndReadResponse());
        assertEquals("ok SMQP", helper3.connectAndReadResponse());
    }


    /**
     * Verifies that a message can be successfully published to a fanout exchange,
     * and that the subscriber receives the correct messages.
     *
     * @throws IOException if there is an error during message publication
     */
    @Test
    @Timeout(value = 1500, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void publish_to_fanout_exchange_successfully() throws IOException {
        subscriber.connectAndReadResponse();
        publisher.connectAndReadResponse();

        subscriber.subscribe(exchangeName, "fanout", queueName, "none");

        publisher.sendCommandAndReadResponse(exchange("fanout", exchangeName));

        publisher.publish("none", "VERIFICATION-MESSAGE-1");
        assertEquals("VERIFICATION-MESSAGE-1", subscriber.readResponse());
        publisher.publish("none", "VERIFICATION-MESSAGE-2");
        assertEquals("VERIFICATION-MESSAGE-2", subscriber.readResponse());
    }


    /**
     * Verifies that a message can be successfully published to a direct exchange
     * with a valid routing key, and the subscriber receives the correct messages.
     *
     * @throws IOException if there is an error during message publication
     */
    @Test
    @Timeout(value = 1500, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void publish_to_direct_exchange_successfully() throws IOException {
        subscriber.connectAndReadResponse();
        publisher.connectAndReadResponse();

        subscriber.subscribe(exchangeName, "direct", queueName, key);
        publisher.sendCommandAndReadResponse(exchange("direct", exchangeName));
        publisher.publish(key, "VERIFICATION-MESSAGE-1");
        assertEquals("VERIFICATION-MESSAGE-1", subscriber.readResponse());
        publisher.publish(key, "VERIFICATION-MESSAGE-2");
        assertEquals("VERIFICATION-MESSAGE-2", subscriber.readResponse());
    }


    /**
     * Verifies that a message with an invalid routing key is discarded by the direct exchange,
     * and a valid message sent afterward is correctly received by the subscriber.
     *
     * @throws IOException if there is an error during message publication
     */
    @Test
    @Timeout(value = 1500, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void publish_to_direct_exchange_discards_invalid_message() throws IOException {
        subscriber.connectAndReadResponse();
        publisher.connectAndReadResponse();

        subscriber.subscribe(exchangeName, "direct", queueName, key);
        publisher.sendCommandAndReadResponse(exchange("direct", exchangeName));
        publisher.publish("invalid.key", "THIS-SHOULD-BE-DISCARDED");
        publisher.publish(key, "VERIFICATION-MESSAGE");

        assertEquals("VERIFICATION-MESSAGE", subscriber.readResponse());
    }


    /**
     * Verifies that a message can be successfully published to the default exchange,
     * and the subscriber receives the correct messages.
     *
     * @throws IOException if there is an error during message publication
     */
    @Test
    @Timeout(value = 1500, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void publish_to_default_exchange_successfully() throws IOException {
        subscriber.connectAndReadResponse();
        publisher.connectAndReadResponse();

        subscriber.sendCommandAndReadResponse(exchange("default", "default"));
        subscriber.sendCommandAndReadResponse(queue(queueName));
        subscriber.sendCommandAndReadResponse(subscribe());

        publisher.sendCommandAndReadResponse(exchange("default", "default"));
        publisher.publish(queueName, "VERIFICATION-MESSAGE-1");
        assertEquals("VERIFICATION-MESSAGE-1", subscriber.readResponse());
        publisher.publish(queueName, "VERIFICATION-MESSAGE-2");
        assertEquals("VERIFICATION-MESSAGE-2", subscriber.readResponse());
    }

    /**
     * Verifies that a message with an invalid routing key is discarded by the default exchange,
     * and a valid message sent afterward is correctly received by the subscriber.
     *
     * @throws IOException if there is an error during message publication
     */
    @Test
    @Timeout(value = 1500, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void publish_to_default_exchange_discards_invalid_message() throws IOException {
        subscriber.connectAndReadResponse();
        publisher.connectAndReadResponse();

        subscriber.sendCommandAndReadResponse(exchange("default", "default"));
        subscriber.sendCommandAndReadResponse(queue(queueName));
        subscriber.sendCommandAndReadResponse(subscribe());

        publisher.sendCommandAndReadResponse(exchange("default", "default"));
        publisher.publish("invalid.key", "THIS-SHOULD-BE-DISCARDED");
        publisher.publish(queueName, "VERIFICATION-MESSAGE");

        assertEquals("VERIFICATION-MESSAGE", subscriber.readResponse());
    }

    /**
     * Verifies that the publisher can bind a queue to a direct exchange before the subscriber subscribes to it,
     * and that the invalid message is discarded while the valid message is received by the subscriber.
     *
     * @throws IOException if there is an error during message publication
     */
    @Test
    @Timeout(value = 1500, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void publish_to_direct_exchange_discards_invalid_message_publisher_binds_queue() throws IOException {
        subscriber.connectAndReadResponse();
        publisher.connectAndReadResponse();

        // Publisher binds queue to exchange
        publisher.sendCommandAndReadResponse(exchange("direct", exchangeName));
        publisher.sendCommandAndReadResponse(queue(queueName));
        publisher.sendCommandAndReadResponse(bind(key));

        // subscriber only subscribes to existing queue
        subscriber.subscribe(queueName);

        publisher.publish("invalid.key", "THIS-SHOULD-BE-DISCARDED");
        publisher.publish(key, "VERIFICATION-MESSAGE");

        assertEquals("VERIFICATION-MESSAGE", subscriber.readResponse());
    }

    /**
     * Verifies that a subscriber can reconnect and subscribe to a fanout exchange,
     * and that invalid messages are discarded before the subscription is active.
     *
     * @throws IOException if there is an error during message publication
     */
    @Test
    @Timeout(value = 1500, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void publish_to_fanout_exchange_subscriber_reconnects_before_activating_subscription() throws IOException {
        subscriber.connectAndReadResponse();


        // Subscriber declares exchange and queue, disconnects before subscribing
        subscriber.sendCommandAndReadResponse(exchange("fanout", exchangeName));
        subscriber.sendCommandAndReadResponse(queue(queueName));
        subscriber.sendCommandAndReadResponse(bind("none"));
        subscriber.sendCommandAndReadResponse(exit());
        subscriber.disconnect();

        // subscriber subscribes to queue that he established previously
        subscriber.connectAndReadResponse();
        subscriber.subscribe(queueName);

        publisher.connectAndReadResponse();
        publisher.sendCommandAndReadResponse(exchange("fanout", exchangeName));
        publisher.publish("whatever.key", "VERIFICATION-MESSAGE");

        assertEquals("VERIFICATION-MESSAGE", subscriber.readResponse());
    }

    /**
     * Verifies that a message can be published to a direct exchange before the subscriber subscribes,
     * and that the message is correctly received after the subscription is established.
     *
     * @throws IOException if there is an error during message publication
     */
    @Test
    @Timeout(value = 1500, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void publish_to_direct_exchange_bound_queue_before_subscription_successfully() throws IOException {
        subscriber.connectAndReadResponse();
        publisher.connectAndReadResponse();

        // Publisher binds queue
        publisher.sendCommandAndReadResponse(exchange("direct", exchangeName));
        publisher.sendCommandAndReadResponse(queue(queueName));
        publisher.sendCommandAndReadResponse(bind(key));

        // Publish to it before subscriber appears
        publisher.publish(key, "VERIFICATION-MESSAGE");

        // Subscribe to the queue after it has been published to
        subscriber.subscribe(queueName);

        assertEquals("VERIFICATION-MESSAGE", subscriber.readResponse());
    }

    /**
     * Verifies that a message published to a direct exchange before queue binding is discarded,
     * and that subsequent valid messages are correctly received after the queue is bound.
     *
     * @throws IOException if there is an error during message publication
     */
    @Test
    @Timeout(value = 1500, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void publish_to_direct_exchange_bind_queue_after_publish_unsuccessfully() throws IOException {
        subscriber.connectAndReadResponse();
        publisher.connectAndReadResponse();

        // Publisher only creates queue, does not bind
        publisher.sendCommandAndReadResponse(exchange("direct", exchangeName));
        publisher.sendCommandAndReadResponse(queue(queueName));

        // Publish to before it is bound, this should not arrive
        publisher.publish(key, "THIS-SHOULD-BE-DISCARDED");

        // Subscriber binds queue and subscribes to it
        subscriber.subscribe(exchangeName, "direct", queueName, key);

        // Publish after it is bound, this should arrive
        publisher.publish(key, "VERIFICATION-MESSAGE");

        assertEquals("VERIFICATION-MESSAGE", subscriber.readResponse());
    }

}
