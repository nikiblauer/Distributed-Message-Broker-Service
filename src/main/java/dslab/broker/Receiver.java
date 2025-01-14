package dslab.broker;

import dslab.broker.enums.ElectionState;
import dslab.broker.enums.ElectionType;

import java.io.*;
import java.net.*;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Receiver implements Runnable {
    private final Broker broker;
    private volatile boolean running;
    private ServerSocket serverSocket;
    private final ExecutorService executor;


    public Receiver(Broker broker) {
        this.broker = broker;
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @Override
    public void run() {
        this.running = true;

        try {
            serverSocket = new ServerSocket(broker.getConfig().electionPort());
            while (running) {
                acceptClientConnections();
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }


    private void acceptClientConnections(){
        try {
            Socket clientSocket = serverSocket.accept();
            executor.submit(() -> handleConnection(clientSocket));
        } catch (SocketException e) {
            if (running) {
                System.err.println("Socket exception: " + e.getMessage());
            }
        } catch (IOException e) {
            System.err.println("Error accepting client connection: " + e.getMessage());
        }
    }

    private void handleConnection(Socket clientSocket) {
        try (clientSocket;
             Scanner in = new Scanner(clientSocket.getInputStream());
             PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

            out.println("ok LEP");
            broker.updateHeartbeat(); // update heartbeat

            while(in.hasNextLine()){
                processClientCommand(in.nextLine(), out);
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }


    }

    private void processClientCommand(String command, PrintWriter out) {
        if (command == null) {
            return;
        }

        String response = parseCommand(command); // Parse and validate the command
        out.println(response);

        // If the response is valid, process the command
        if (isResponseValid(response)) {
            broker.handleMessage(command);
        }
    }

    private boolean isResponseValid(String response) {
        return response.startsWith("ok") || response.startsWith("ack") ||
                response.startsWith("pong") || response.startsWith("vote");
    }

    private String parseCommand(String command) {
        String[] parts = command.split(" ");
        String cmd = parts[0].toLowerCase();

        return switch (cmd) {
            case "elect" -> handleElectCommand(parts);
            case "declare" -> handleDeclareCommand(parts);
            case "ping" -> handlePingCommand(parts);
            default -> "error protocol error";
        };
    }

    private String handleElectCommand(String[] parts) {
        broker.setElectionState(ElectionState.CANDIDATE);

        if (parts.length != 2) {
            return "error usage: elect <id>";
        }


        if (broker.getElectionType() == ElectionType.RAFT) {
            if (broker.getCurrentVote() == -1) {
                broker.vote(Integer.parseInt(parts[1]));
            }
            return "vote " + broker.getId() + " " + broker.getCurrentVote();
        }

        return "ok";
    }

    private String handleDeclareCommand(String[] parts) {
        if (parts.length != 2) {
            return "error usage: declare <id>";
        }
        return "ack " + broker.getId();
    }

    private String handlePingCommand(String[] parts) {
        if (parts.length != 1) {
            return "error usage: ping";
        }
        return "pong";
    }



    private void closeServerSocket() {
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                System.out.println("Error closing server socket: " + e.getMessage());
            }
        }
    }

    public void shutdown() {
        this.running = false;

        closeServerSocket();
        if (executor != null) {
            executor.shutdown();
        }
    }

}