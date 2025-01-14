package dslab.broker;

import dslab.broker.enums.ElectionType;
import dslab.config.BrokerConfig;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Sender {
    private final Broker broker;

    private final Map<Integer, Socket> heartbeatConnections = new ConcurrentHashMap<>();
    private final Map<Integer, PrintWriter> heartbeatWriters = new ConcurrentHashMap<>();
    private Timer heartbeatTimer; // Reference to the heartbeat timer
    private boolean running;

    public Sender(Broker broker) {
        this.broker = broker;
        this.running = true;
    }

    public int sendMessage(String message) {
        if (!running){
            return 1;
        }

        boolean success = false;
        int votes = 0;


        for (int i = 0; i < broker.getConfig().electionPeerIds().length; i++) {
            String host = broker.getConfig().electionPeerHosts()[i];
            int port = broker.getConfig().electionPeerPorts()[i];

            if ((broker.getElectionType() == ElectionType.BULLY) && message.startsWith("elect"))
            {
                if (broker.getId() > broker.getConfig().electionPeerIds()[i]){
                    continue;
                }
            }


            try (Socket socket = new Socket(host, port);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {


                String response = in.readLine();


                if ((response == null) || !response.equals("ok LEP")){
                    continue;
                }

                out.println(message);
                response = in.readLine();
                if (response != null){
                    success = true;
                    if (broker.getElectionType() == ElectionType.RING){
                        break;
                    } else if (broker.getElectionType() == ElectionType.RAFT){
                        if (response.startsWith("vote")){
                            String[] parts = response.split(" ");
                            int vote = Integer.parseInt(parts[2]);
                            if (vote == broker.getId()){
                                votes += 1;
                            }
                        }

                    }
                }
            } catch (IOException e) {
                System.out.println("Node " + broker.getId() + ": Unable to contact Node " + broker.getConfig().electionPeerIds()[i] + " at port " + port);
            }

        }

        if (broker.getElectionType() == ElectionType.RAFT){
            return votes;
        }

        return success ? 1 : 0;
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
            } catch (IOException e) {
                System.out.println("Unable to establish connection to Node " + peerID + " at port " + port);
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
        }, 0, 20);
    }

    public void closeConnections() {
        // Stop the heartbeat timer, when no longer leader
        if (heartbeatTimer != null){
            heartbeatTimer.cancel();
        }

        for (Socket socket : heartbeatConnections.values()) {
            try {
                socket.close();
            } catch (IOException e) {
                System.out.println("Node " + broker.getId() + ": Error closing connection");
                System.out.println(e.getMessage());
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
