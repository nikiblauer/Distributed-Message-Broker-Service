package dslab.assignment2.broker;

import dslab.util.Constants;
import dslab.util.Global;
import dslab.util.helper.TelnetClientHelper;
import dslab.util.threads.PublisherThread;
import dslab.util.threads.SubscriberThread;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static dslab.util.CommandBuilder.*;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class SingleExchangeConcurrencyTest extends BaseSingleBrokerTest {

    @Override
    protected void initTelnetClientHelpers() throws IOException {
        // Not used
    }

    @Override
    protected void closeTelnetClientHelpers() throws IOException {
        // Not used
    }

    /**
     * Tests a concurrency scenario where multiple publishers and subscribers interact with a direct exchange.
     * <p>
     * - The test initializes:
     * - Two queues bound to different routing keys in a direct exchange.
     * - Two subscribers, each subscribed to one of the queues.
     * - Four publishers sending messages to the exchange, with each publisher publishing 100 messages.
     * - Each queue receives messages.
     * <p>
     * - The subscribers receive messages concurrently, and the test verifies that all messages are delivered correctly
     * to the subscribers according to the routing keys.
     *
     * @throws InterruptedException if the thread is interrupted while waiting for publishers or subscribers to finish
     * @throws IOException          if there is an error in the TCP communication with the broker
     */
    @Test
    @Timeout(value = 6000, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void publish_to_direct_exchange() throws InterruptedException, IOException {
        final int numberOfSubscribers = 2;
        final int numberOfPublishers = 4;
        final int messagesPerPublisher = 100;
        final int numOfMsgPerSubscriber = numberOfPublishers * messagesPerPublisher / numberOfSubscribers;
        final String exchangeName = String.format("exchange-%s", Global.SECURE_STRING_GENERATOR.getSecureString());
        final String queueName1 = String.format("queue-%s", Global.SECURE_STRING_GENERATOR.getSecureString());
        final String queueName2 = String.format("queue-%s", Global.SECURE_STRING_GENERATOR.getSecureString());
        final String key1 = String.format("rk-%s", Global.SECURE_STRING_GENERATOR.getSecureString());
        final String key2 = String.format("rk-%s", Global.SECURE_STRING_GENERATOR.getSecureString());

        // Setup exchange, 2 Queues (with binding)
        final TelnetClientHelper helper = new TelnetClientHelper(Constants.LOCALHOST, config.brokerPort());
        helper.connectAndReadResponse();
        helper.sendCommandAndReadResponse(exchange("direct", exchangeName));
        helper.sendCommandAndReadResponse(queue(queueName1));
        helper.sendCommandAndReadResponse(bind(key1));
        helper.sendCommandAndReadResponse(queue(queueName2));
        helper.sendCommandAndReadResponse(bind(key2));
        helper.disconnect();

        // Init Subscribers
        final Set<SubscriberThread> subscribers = new HashSet<>();
        subscribers.add(new SubscriberThread(config.brokerPort(), "direct", exchangeName, queueName1, numOfMsgPerSubscriber));
        subscribers.add(new SubscriberThread(config.brokerPort(), "direct", exchangeName, queueName2, numOfMsgPerSubscriber));

        // Init publishers
        List<PublisherThread> publishers = new ArrayList<>();
        for (int i = 0; i < numberOfPublishers; i++) {
            publishers.add(new PublisherThread(config.brokerPort(), exchangeName, "direct", new String[]{key1, key2}, messagesPerPublisher));
        }

        // Start publishers
        for (PublisherThread p : publishers) p.start();

        // Start subscribers
        for (SubscriberThread s : subscribers) s.start();

        // Wait until all Subscribers have finished
        for (SubscriberThread s : subscribers) s.join();

        // check if all messages have arrived
        for (SubscriberThread s : subscribers) {
            assertThat(s.getReceivedMessages().size()).isEqualTo(numOfMsgPerSubscriber);
        }
    }


    /**
     * Tests a concurrency scenario where multiple publishers and subscribers interact with a fanout exchange.
     * <p>
     * - The test initializes:
     * - A fanout exchange with two queues.
     * - Two subscribers subscribed to the queues.
     * - Two publishers sending messages to the exchange, with each publisher publishing 100 messages.
     * <p>
     * - Since it's a fanout exchange, all messages are broadcasted to both queues, and subscribers should receive all messages.
     * - The test verifies that all subscribers receive the correct number of messages.
     *
     * @throws InterruptedException if the thread is interrupted while waiting for publishers or subscribers to finish
     * @throws IOException          if there is an error in the Telnet communication with the broker
     */
    @Test
    @Timeout(value = 6000, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void publish_to_fanout_exchange() throws InterruptedException, IOException {
        final int numberOfPublishers = 2;
        final int messagesPerPublisher = 100;
        final int numOfMsgPerSubscriber = numberOfPublishers * messagesPerPublisher;
        final String exchangeName = String.format("exchange-%s", Global.SECURE_STRING_GENERATOR.getSecureString());
        final String queueName1 = String.format("queue-%s", Global.SECURE_STRING_GENERATOR.getSecureString());
        final String queueName2 = String.format("queue-%s", Global.SECURE_STRING_GENERATOR.getSecureString());

        // Setup exchange, 2 Queues (with binding)
        final TelnetClientHelper helper = new TelnetClientHelper(Constants.LOCALHOST, config.brokerPort());
        helper.connectAndReadResponse();
        helper.sendCommandAndReadResponse(exchange("fanout", exchangeName));
        helper.sendCommandAndReadResponse(queue(queueName1));
        helper.sendCommandAndReadResponse(bind("whatever"));
        helper.sendCommandAndReadResponse(queue(queueName2));
        helper.sendCommandAndReadResponse(bind("whatever"));
        helper.disconnect();

        // Init Subscribers
        final Set<SubscriberThread> subscribers = new HashSet<>();
        subscribers.add(new SubscriberThread(config.brokerPort(), "fanout", exchangeName, queueName1, numOfMsgPerSubscriber));
        subscribers.add(new SubscriberThread(config.brokerPort(), "fanout", exchangeName, queueName2, numOfMsgPerSubscriber));

        // Init publishers
        List<PublisherThread> publishers = new ArrayList<>();
        for (int i = 0; i < numberOfPublishers; i++) {
            publishers.add(new PublisherThread(config.brokerPort(), exchangeName, "fanout", new String[]{"whatever"}, messagesPerPublisher));
        }

        // Start publishers
        for (PublisherThread p : publishers) p.start();

        // Start subscribers
        for (SubscriberThread s : subscribers) s.start();

        // Wait until all Subscribers have finished
        for (SubscriberThread s : subscribers) s.join();

        // check if all messages have arrived
        for (SubscriberThread s : subscribers) {
            assertThat(s.getReceivedMessages().size()).isEqualTo(numOfMsgPerSubscriber);
        }
    }

}
