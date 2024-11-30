package dslab.broker;

import dslab.ComponentFactory;
import dslab.config.BrokerConfig;

public class Broker implements IBroker {

    public Broker(BrokerConfig config) {

    }

    @Override
    public void run() {

    }

    @Override
    public int getId() {
        return 0;
    }

    @Override
    public void initiateElection() {

    }

    @Override
    public int getLeader() {
        return 0;
    }

    @Override
    public void shutdown() {

    }

    public static void main(String[] args) {
        ComponentFactory.createBroker(args[0]).run();
    }
}
