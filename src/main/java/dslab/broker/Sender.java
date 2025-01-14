package dslab.broker;

import dslab.broker.enums.ElectionType;
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
    private Timer heartbeatTimer;
    private boolean running;

    public Sender(Broker broker) {
        this.broker = broker;
        this.peerIds = this.broker.getConfig().electionPeerIds();
        this.peerHosts = this.broker.getConfig().electionPeerHosts();
        this.peerPorts = this.broker.getConfig().electionPeerPorts();
        this.running = true;
    }

    public int sendMessage(String message) {
        if (!running){
            return 1;
        }

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


                if (!"ok LEP".equals(in.readLine())) {
                    continue;
                }

                out.println(message);
                String response = in.readLine();

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
        if (!running){
            return;
        }

        closeConnections(); // Ensure no stale connections

        for (int i = 0; i < peerIds.length; i++) {
            try {
                establishConnection(peerIds[i], peerHosts[i], peerPorts[i]);
            } catch (IOException e) {
                System.out.println("Unable to establish connection to Node " + peerIds[i]);
            }
        }
        startHeartbeat();
    }

    private void establishConnection(int peerId, String host, int port) throws IOException {
        Socket socket = new Socket(host, port);
        PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);

        heartbeatConnections.put(peerId, socket);
        heartbeatWriters.put(peerId, writer);
    }


    private void startHeartbeat() {
        // Send periodic heartbeats
        heartbeatTimer = new Timer();
        heartbeatTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                sendHeartbeat();
            }
        }, 0, 20);
    }

    private void sendHeartbeat() {
        heartbeatWriters.values().forEach(writer -> writer.println("ping"));
    }

    private void stopHeartbeat() {
        if (heartbeatTimer != null) {
            heartbeatTimer.cancel();
        }
    }

    public void closeConnections() {
        stopHeartbeat();

        heartbeatConnections.values().forEach(socket -> {
            try {
                socket.close();
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
        });

        heartbeatConnections.clear();
        heartbeatWriters.clear();

    }

    public void shutdown() {
        this.running = false;
        closeConnections();
    }
}
