package dslab.broker;

import dslab.config.BrokerConfig;

import java.io.*;
import java.net.*;
import java.util.*;

public class Sender {
    private final Broker broker;

    private final Map<Integer, Socket> heartbeatConnections = new HashMap<>();
    private final Map<Integer, PrintWriter> heartbeatWriters = new HashMap<>();
    private Timer heartbeatTimer; // Reference to the heartbeat timer
    private boolean running;

    public Sender(Broker broker) {
        this.broker = broker;
        this.running = true;
    }

    public boolean sendMessage(String message) {
        if (!running){
            return false;
        }

        boolean success = false;
        for (int i = 0; i < broker.getConfig().electionPeerIds().length; i++) {
            String host = broker.getConfig().electionPeerHosts()[i];
            int port = broker.getConfig().electionPeerPorts()[i];

            //System.out.println("Sending message: " + message + " to " + host + ":" + port);

            try (Socket socket = new Socket(host, port);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {


                String response = in.readLine();
                out.println(message);
                response = in.readLine();
                if (response != null){
                    if (response.equals("ok") || response.startsWith("ack")){
                        success = true;
                        break;
                    }
                }


            } catch (IOException e) {
                //System.out.println("Node " + broker.getId() + ": Unable to contact Node " + broker.getConfig().electionPeerIds()[i] + " at port " + port);
            }



        }
        return success;
    }

    public void establishConnectionsForLeader() {
        if (!running){
            return;
        }
        closeConnections(); // Ensure no stale connections

        for (int i = 0; i < broker.getConfig().electionPeerIds().length; i++) {
            String host = broker.getConfig().electionPeerHosts()[i];
            int port = broker.getConfig().electionPeerPorts()[i];
            int peerID = broker.getConfig().electionPeerIds()[i];


            try {
                Socket socket = new Socket(host, port);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);

                heartbeatConnections.put(peerID, socket);
                heartbeatWriters.put(peerID, writer);

                //System.out.println("Leader " + broker.getId() + " established persistent connection to Node " + peerID + " at port " + port);
            } catch (IOException e) {
                //System.out.println("Leader " + broker.getId() + ": Unable to establish connection to Node " + peerID + " at port " + port);
            }

        }

        // Send periodic heartbeats
        heartbeatTimer = new Timer();
        heartbeatTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                for (PrintWriter writer : heartbeatWriters.values()) {
                    writer.println("ping");
                }
            }
        }, 0, 50);
    }

    public void closeConnections() {
        // Stop the heartbeat timer

        for (Socket socket : heartbeatConnections.values()) {
            try {
                socket.close();
            } catch (IOException e) {
                //System.out.println("Node " + broker.getId() + ": Error closing connection");
            }
        }
        heartbeatConnections.clear();
        heartbeatWriters.clear();
    }

    public void shutdown() {
        this.running = false;
        closeConnections();
    }
}
