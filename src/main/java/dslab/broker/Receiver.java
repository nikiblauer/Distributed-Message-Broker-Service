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
                        Socket clientSocket = serverSocket.accept(); // Accept a connection
                        Thread.ofVirtual().start(() -> handleConnection(clientSocket)); // Use a virtual thread to handle the connection
                    } catch (SocketException e) {
                        if (!running) {

                        } else {
                            e.printStackTrace();
                        }
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
                e.printStackTrace();
            }
        }
    }

    private void handleConnection(Socket clientSocket) {


        try (clientSocket;
             Scanner in = new Scanner(clientSocket.getInputStream());
             PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

            // Respond with "ok LEP" on a new connection
            out.println("ok LEP");

            // Read the command from the client

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
                if (parts.length != 2) {
                    return "error usage: elect <id>";
                }
                if (broker.getElectionType() == ElectionType.RAFT) {
                    if (broker.getElectionState() != ElectionState.CANDIDATE){
                        broker.incTerm();
                        broker.currentVote = Integer.parseInt(parts[1]);
                        return "vote " + broker.getId() + " " + parts[1];
                    } else {
                        return "vote " + broker.getId() + " " + broker.currentVote;
                    }
                }
                return "ok";

            case "declare":
                if (parts.length != 2) {
                    return "error usage: declare <id>";
                }
                int leaderId = Integer.parseInt(parts[1]);

                return "ack" + leaderId;

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