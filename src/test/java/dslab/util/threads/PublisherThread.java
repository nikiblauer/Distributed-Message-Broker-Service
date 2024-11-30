package dslab.util.threads;

import dslab.util.Constants;
import dslab.util.helper.TelnetClientHelper;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class PublisherThread extends Thread {

    final private TelnetClientHelper helper;

    private final int messageCount;
    final private String exchangeName;
    final private String exchangeType;
    final private String[] routingKeys;


    public PublisherThread(int remotePort, String exchangeName, String exchangeType, String[] routingKeys, int messageCount) {
        this.helper = new TelnetClientHelper(Constants.LOCALHOST, remotePort);
        this.exchangeName = exchangeName;
        this.exchangeType = exchangeType;
        this.routingKeys = routingKeys;
        this.messageCount = messageCount;
    }

    @Override
    public void run() {
        try {
            helper.connectAndReadResponse();
            helper.sendCommandAndReadResponse(String.format("exchange %s %s", exchangeType, exchangeName));

            int routingKeyIndex = 0;

            for (int i = 0; i < messageCount; i++) {
                String msg = String.format("publish %s %s",
                        routingKeys[(routingKeyIndex++) % routingKeys.length],
                        String.format("Thread %d, Message %d", Thread.currentThread().threadId(), i + 1));
                log.debug("Sending Message: {}", msg);
                helper.sendCommandAndReadResponse(msg);
            }
            helper.disconnect();
        } catch (IOException e) {
            //  stop everything and shut down
        } finally {
            shutdown();
        }
    }

    public void shutdown() {
        try {
            helper.disconnect();
        } catch (IOException e) {
            log.debug("Unable to shutdown", e);
        }
    }
}
