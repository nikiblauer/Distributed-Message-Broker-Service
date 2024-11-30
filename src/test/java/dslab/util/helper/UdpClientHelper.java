package dslab.util.helper;

import java.io.IOException;
import java.net.*;

public class UdpClientHelper {

    private final InetSocketAddress socketAddress;
    private final DatagramSocket socket;

    public UdpClientHelper(String hostname, int port) throws SocketException {
        this.socket = new DatagramSocket();
        this.socketAddress = new InetSocketAddress(hostname, port);
    }

    public void send(String msg) throws IOException {
        byte[] buffer = msg.getBytes();

        DatagramPacket packet = new DatagramPacket(buffer, buffer.length, socketAddress);

        socket.send(packet);
    }

    public void disconnect() {
        if (socket != null && !socket.isClosed()) socket.close();
    }
}
