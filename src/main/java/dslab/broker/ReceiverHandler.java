package dslab.broker;

import dslab.broker.enums.ElectionState;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Arrays;
import java.util.Scanner;

public class ReceiverHandler implements Runnable {
    private final Socket clientSocket;
    private final PrintWriter out;
    private final Scanner scanner;
    private boolean running;
    private final Receiver receiver;

    public ReceiverHandler(Receiver receiver, Socket clientSocket) {
        this.clientSocket = clientSocket;
        try {
            out = new PrintWriter(clientSocket.getOutputStream(), true);
            scanner = new Scanner(clientSocket.getInputStream());
        } catch (IOException e) {
            System.err.println("error when opening streams of socket");
            throw new RuntimeException(e);
        }

        this.receiver = receiver;
    }

    @Override
    public void run() {
        out.println("ok LEP");
        handleIncomingMessage();
    }

    private void handleIncomingMessage() {

        String input = scanner.nextLine();

        String[] tokens = input.split(" ");
        String command = tokens[0].toLowerCase();
        String[] args = Arrays.copyOfRange(tokens, 1, tokens.length);

        switch (command) {
            case "ping": {
                handlePing(args);
                break;
            }
            case "elect": {
                handleElect(args);
                break;
            }
            case "vote": {
                break;
            }
            case "declare": {
                handleDeclare(args);
                break;
            }
            default: {
                out.println("error protocol error");
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        shutdown();

    }

    private void handleElect(String[] args){
        if (args.length != 1) {
            out.println("error usage: elect <id>");
        }

        out.println("ok");

        synchronized (receiver.getBroker()) {
            receiver.getBroker().setElectionState(ElectionState.CANDIDATE);
        }

        int id = Integer.parseInt(args[0]);

        if (id == receiver.getBroker().getId()){
            synchronized(receiver.getBroker().getSender()) {
                receiver.getBroker().getSender().declare(id);
            }
        }
        else if (id < receiver.getBroker().getId()){
            synchronized (receiver.getBroker().getSender()) {
                receiver.getBroker().getSender().elect(receiver.getBroker().getId());
            }
        } else {
            synchronized (receiver.getBroker().getSender()) {
                receiver.getBroker().getSender().elect(id);
            }
        }
    }

    private void handleDeclare(String[] args) {
        if (args.length != 1) {
            out.println("error usage: declare <id>");
        }

        int senderId = Integer.parseInt(args[0]);

        synchronized (receiver.getBroker().getReceiver()) {
            this.receiver.getBroker().setElectionState(ElectionState.FOLLOWER);
            this.receiver.getBroker().setLeader(senderId);
        }

        if (senderId == receiver.getBroker().getId()){
            out.println("ok");
        }
        else {
            out.println("ack " + senderId);
            synchronized (receiver.getBroker().getSender()) {
                receiver.getBroker().getSender().declare(senderId);
            }
        }
        this.receiver.setLeaderElected(true);

    }

    private void handlePing(String[] args){
        out.println("pong");
        receiver.updateHeartbeat();
    }

    private void shutdown() {
        this.running = false;

        try {
            clientSocket.close();
        } catch (IOException e) {
            //throw new RuntimeException(e);
        }
    }




}
