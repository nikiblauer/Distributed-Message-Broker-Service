package dslab.util.threads;

import dslab.util.Constants;
import dslab.util.helper.TelnetClientHelper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

@Slf4j
public class SubscriberThread extends Thread {

    private final TelnetClientHelper helper;
    private final String exchangeName;
    private final String exchangeType;
    private final String queueName;
    private final int expectedMessages;

    private final List<String> receivedMessages = new LinkedList<>();

    public SubscriberThread(int remotePort, String exchangeType, String exchangeName,
                            String queueName, int expectedMessages) {
        helper = new TelnetClientHelper(Constants.LOCALHOST, remotePort);
        this.exchangeName = exchangeName;
        this.exchangeType = exchangeType;
        this.queueName = queueName;
        this.expectedMessages = expectedMessages;
    }

    @Override
    public void run() {
        try {
            helper.connectAndReadResponse();
            helper.sendCommandAndReadResponse("exchange %s %s".formatted(exchangeType, exchangeName));
            helper.sendCommandAndReadResponse("queue %s".formatted(queueName));
            helper.sendCommandAndReadResponse("subscribe");
            log.debug("Subscribing to: {}", queueName);

            for (int i = 0; i < expectedMessages; i++) {
                String s = helper.readResponse();
                log.debug("Received message: {}", s);
                receivedMessages.add(s);
            }
        } catch (IOException e) {
            //  stop everything and shut down
            log.debug("Exception when subscribing. Shutting down", e);
        } finally {
            shutdown();
        }
        log.debug("Subscriber Finished");
    }

    private void shutdown() {
        try {
            helper.disconnect();
        } catch (IOException e) {
            log.debug("Unable to shutdown", e);
            // DO nothing
        }
    }

    public List<String> getReceivedMessages() {
        return receivedMessages;
    }
}
