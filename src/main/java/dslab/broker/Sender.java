package dslab.broker;

import dslab.broker.enums.ElectionType;
import dslab.config.BrokerConfig;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Sender {
    private final Broker broker;
    private final int[] peerIds;
    private final String[] peerHosts;
    private final int[] peerPorts;

    private final Map<Integer, Socket> heartbeatConnections = new ConcurrentHashMap<>();
    private final Map<Integer, PrintWriter> heartbeatWriters = new ConcurrentHashMap<>();
    private Timer heartbeatTimer; // Reference to the heartbeat timer

    public Sender(Broker broker) {
        this.broker = broker;
        this.peerIds = broker.getConfig().electionPeerIds();
        this.peerHosts = broker.getConfig().electionPeerHosts();
        this.peerPorts = broker.getConfig().electionPeerPorts();
    }

    public int sendMessage(String message) {
        boolean success = false;
        int votes = 0;


        for (int i = 0; i < peerIds.length; i++) {

            if ((broker.getElectionType() == ElectionType.BULLY) && message.startsWith("elect"))
            {
                if (broker.getId() > broker.getConfig().electionPeerIds()[i]){
                    continue;
                }
            }


            try (Socket socket = new Socket(peerHosts[i], peerPorts[i]);
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
                System.out.println("Unable to contact Node " + peerIds[i]);
            }

        }

        if (broker.getElectionType() == ElectionType.RAFT){
            return votes;
        }

        return success ? 1 : 0;
    }

    public void establishConnectionsForLeader() {
        closeConnections(); // Ensure no stale connections

        for (int i = 0; i < peerIds.length; i++) {
            try {
                Socket socket = new Socket(peerHosts[i], peerPorts[i]);
                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);

                heartbeatConnections.put(peerIds[i], socket);
                heartbeatWriters.put(peerIds[i], writer);
            } catch (IOException e) {
                System.out.println("Unable to establish connection to Node " + peerIds[i]);
            }

        }

        startHeartbeatTimer();
    }

    private void startHeartbeatTimer() {
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

    private void stopHeartbeatTimer() {
        // Stop the heartbeat timer, when no longer leader
        if (heartbeatTimer != null){
            heartbeatTimer.cancel();
        }
    }

    public void closeConnections() {
        stopHeartbeatTimer();

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
        closeConnections();
    }
}