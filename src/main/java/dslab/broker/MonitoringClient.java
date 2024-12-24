package dslab.broker;

import java.io.IOException;
import java.net.*;

public class MonitoringClient {
    private final DatagramSocket socket;
    private final InetAddress address;
    private final int port;
    private final String brokerHost;
    private final int brokerPort;
    private final boolean valid;

    public MonitoringClient(String host, int port, String brokerHost, int brokerPort) {
        valid = port >= 1 && port <= 65535;
        this.port = port;
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;

        try {
            socket = new DatagramSocket();
            address = InetAddress.getByName(host);
        } catch (SocketException | UnknownHostException e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public synchronized void sendLog(String routingKey){
        if(!valid){
            return;
        }

        String message = brokerHost + ":" + brokerPort + " " + routingKey;
        byte[] sendData = message.getBytes();

        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, address, port);

        // Send the packet
        try {
            socket.send(sendPacket);
        } catch (IOException e) {
            System.out.println(e.getMessage());

            throw new RuntimeException(e);
        }
    }

    public synchronized void shutdown(){
        socket.close();
    }
}
