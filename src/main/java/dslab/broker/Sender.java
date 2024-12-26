package dslab.broker;

import dslab.broker.enums.ElectionState;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Scanner;

public class Sender implements Runnable {

    private boolean running;
    private final Broker broker;
    private final LinkedList<Socket> sockets = new LinkedList<>();



    public Sender(Broker broker) {
        this.running = true;
        this.broker = broker;
    }


    private void connectToPeers() {
        for (int i = 0; i < broker.getConfig().electionPeerIds().length; i++) {
            Socket socket;
            try {
                socket = new Socket(broker.getConfig().electionPeerHosts()[i], broker.getConfig().electionPeerPorts()[i]);
                Scanner in = new Scanner(socket.getInputStream());
                String connectResponse = in.nextLine();
                if (connectResponse.equalsIgnoreCase("ok LEP")){
                    sockets.add(socket);
                }
            } catch (IOException e) {
                //throw new RuntimeException(e);
            }
        }
    }

    public void ping() {
        connectToPeers();
        for (Socket socket : sockets) {
            PrintWriter out = null;
            try {
                out = new PrintWriter(socket.getOutputStream(), true);
                out.println("ping");
            } catch (IOException e) {
                //throw new RuntimeException(e);
            }
        }
        disconnectFromPeers();
    }


    public void elect(int id) {
        for (int i = 0; i < broker.getConfig().electionPeerIds().length; i++) {
            Socket socket;
            try {
                socket = new Socket(broker.getConfig().electionPeerHosts()[i], broker.getConfig().electionPeerPorts()[i]);
                sockets.add(socket);
                Scanner in = new Scanner(socket.getInputStream());
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                String connectResponse = in.nextLine();
                if (connectResponse.equalsIgnoreCase("ok LEP")){
                    out.println("elect " + id);
                    if (in.nextLine().equals("ok")){
                        break;
                    }

                }
            } catch (IOException e) {
                //throw new RuntimeException(e);
            }
        }
        disconnectFromPeers();
    }

    /*
    public void elect(int id) {
        connectToPeers();
        for (Socket socket : sockets) {
            PrintWriter out = null;
            try {
                out = new PrintWriter(socket.getOutputStream(), true);
                out.println("elect " + id);
                Scanner in = new Scanner(socket.getInputStream());
                if (in.nextLine().equals("ok")){
                    break;
                }
            } catch (IOException e) {
                //throw new RuntimeException(e);
            }
        }
        disconnectFromPeers();
    }

     */



    public void declare(int id) {
        this.broker.setLeader(id);
        for (int i = 0; i < broker.getConfig().electionPeerIds().length; i++) {
            Socket socket;
            try {
                socket = new Socket(broker.getConfig().electionPeerHosts()[i], broker.getConfig().electionPeerPorts()[i]);
                sockets.add(socket);
                Scanner in = new Scanner(socket.getInputStream());
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                String connectResponse = in.nextLine();
                if (connectResponse.equalsIgnoreCase("ok LEP")) {
                    out.println("declare " + id);
                    String response = in.nextLine();
                    if (response.equals("ok") || response.startsWith("ack")) {
                        break;
                    }
                }
            } catch (IOException e) {
            //throw new RuntimeException(e);
            }
        }

        disconnectFromPeers();
    }



    /*public void declare(int id) {
        this.broker.setLeader(id);

        connectToPeers();
        for (Socket socket : sockets) {
            PrintWriter out = null;
            try {
                out = new PrintWriter(socket.getOutputStream(), true);
                out.println("declare " + id);
                Scanner in = new Scanner(socket.getInputStream());
                if (in.nextLine().equals("ok")){
                    break;
                }
            } catch (IOException e) {
                //throw new RuntimeException(e);
            }
        }
        disconnectFromPeers();
    }

     */



    private void disconnectFromPeers() {
        for (Socket socket : sockets) {
            try {
                socket.close();
            } catch (IOException e) {
                //throw new RuntimeException(e);
            }
        }
        sockets.clear();
    }


    @Override
    public void run() {
        while (running) {
            if (broker.getElectionState() == ElectionState.LEADER){
                ping();
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

    }



    public void shutdown() {
        running = false;
    }

}
