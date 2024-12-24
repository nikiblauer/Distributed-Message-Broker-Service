package dslab.monitoring;

import dslab.ComponentFactory;
import dslab.config.MonitoringServerConfig;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MonitoringServer implements IMonitoringServer {

    private final DatagramSocket socket;
    private boolean running;
    private int totalMessagesReceived;
    private final Map<String, Map<String, Integer>> statistics;


    public MonitoringServer(MonitoringServerConfig config) {
        try {
            this.socket = new DatagramSocket(config.monitoringPort());
            this.running = true;
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
        totalMessagesReceived = 0;
        statistics = new HashMap<>();
    }

    @Override
    public void run() {
        byte[] receiveBuffer = new byte[1024];

        while (running) {
            DatagramPacket receivePacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
            try {
                socket.receive(receivePacket);
            } catch (IOException e) {
                running = false;
                break;
            }

            String receivedData = new String(receivePacket.getData(), 0, receivePacket.getLength());
            String regex = "^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}:[0-9]{1,5} \\S+\\s*$";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(receivedData);


            if (matcher.matches()) {
                String[] parts = receivedData.split("\\s+");
                String ipPort = parts[0];
                String routingKey = parts[1];
                addStatistic(ipPort, routingKey);
                totalMessagesReceived++;
            }
        }
    }

    private void addStatistic(String ipPort, String routingKey) {
        statistics.putIfAbsent(ipPort, new HashMap<>());
        Map<String, Integer> routingKeyCounts = statistics.get(ipPort);
        routingKeyCounts.put(routingKey, routingKeyCounts.getOrDefault(routingKey, 0) + 1);
    }


    @Override
    public void shutdown() {
        socket.close();
    }

    @Override
    public int receivedMessages() {
        return totalMessagesReceived;
    }

    @Override
    public String getStatistics() {
        StringBuilder sb = new StringBuilder();

        statistics.forEach((server, routingKeyCounts) -> {
            sb.append("Server ").append(server).append("\n");
            routingKeyCounts.forEach((routingKey, count) ->
                    sb.append("\t").append(routingKey).append(" ").append(count).append("\n")
            );
        });

        return sb.toString();
    }

    public static void main(String[] args) {
        ComponentFactory.createMonitoringServer(args[0]).run();
    }
}
