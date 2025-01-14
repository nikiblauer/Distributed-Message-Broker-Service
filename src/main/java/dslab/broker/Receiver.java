package dslab.broker;

import dslab.broker.enums.ElectionState;
import dslab.broker.enums.ElectionType;

import java.io.*;
import java.net.*;
import java.util.Scanner;

public class Receiver {
    private final Broker broker;
    private volatile boolean running;
    private ServerSocket serverSocket; // Reference to the server socket for proper shutdown

    public Receiver(Broker broker) {
        this.broker = broker;
    }

    public void start() {
        this.running = true;

        new Thread(() -> {
            try {
                serverSocket = new ServerSocket(broker.getConfig().electionPort());
                while (running) {
                    try {
                        Socket clientSocket = serverSocket.accept();
                        Thread.ofVirtual().start(() -> handleConnection(clientSocket)); // Use a virtual thread to handle the connection
                    } catch (SocketException e) {

                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    public void shutdown() {
        this.running = false;

        // Close the server socket
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                System.out.println("Error closing server socket: " + e.getMessage());
            }
        }
    }

    private void handleConnection(Socket clientSocket) {
        try (clientSocket;
             Scanner in = new Scanner(clientSocket.getInputStream());
             PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

            out.println("ok LEP");

            broker.heartbeatReceived = true; // update heartbeat
            while(in.hasNextLine()){
                String command = in.nextLine();
                if (command != null) {
                    String response = parseCommand(command); // Parse and validate the command
                    out.println(response);

                    // If the response is valid, process the command
                    if (response.startsWith("ok") || response.startsWith("ack") || response.startsWith("pong") || response.startsWith("vote")) {
                        broker.handleMessage(command);
                    }

                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    private String parseCommand(String command) {
        String[] parts = command.split(" ");
        String cmd = parts[0].toLowerCase();

        switch (cmd) {
            case "elect":
                broker.electionState = ElectionState.CANDIDATE;
                if (parts.length != 2) {
                    return "error usage: elect <id>";
                }
                if (broker.getElectionType() == ElectionType.RAFT) {
                    if (!broker.hasVoted){
                        broker.currentVote = Integer.parseInt(parts[1]);
                        broker.hasVoted = true;
                    }
                    return "vote " + broker.getId() + " " + broker.currentVote;
                }
                return "ok";

            case "declare":
                if (parts.length != 2) {
                    return "error usage: declare <id>";
                }

                return "ack " + broker.getId();

            case "ping":
                if (parts.length != 1) {
                    return "error usage: ping";
                }
                return "pong";

            default:
                return "error protocol error";
        }
    }
}