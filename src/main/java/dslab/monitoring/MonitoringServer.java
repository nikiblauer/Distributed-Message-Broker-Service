package dslab.monitoring;

import dslab.ComponentFactory;
import dslab.config.MonitoringServerConfig;

public class MonitoringServer implements IMonitoringServer {


    public MonitoringServer(MonitoringServerConfig config) {

    }

    @Override
    public void run() {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public int receivedMessages() {
        return 0;
    }

    @Override
    public String getStatistics() {
        return "";
    }

    public static void main(String[] args) {
        ComponentFactory.createMonitoringServer(args[0]).run();
    }
}
