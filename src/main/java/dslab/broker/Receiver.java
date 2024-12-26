package dslab.broker;

import dslab.IServer;
import dslab.config.BrokerConfig;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Receiver implements IServer {
    private final ServerSocket serverSocket;
    private BrokerConfig config;
    private final ExecutorService executor;
    private volatile boolean running;
    private volatile long lastHeartbeat;
    private Broker broker;
    private boolean leaderElected;

    public Receiver(Broker broker){
        this.broker = broker;
        this.config = broker.getConfig();
        try {
            this.serverSocket = new ServerSocket(this.config.electionPort());
        } catch (IOException e) {
            System.err.println("error creating server socket: " + e.getMessage());
            throw new RuntimeException(e);
        }

        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.running = true;
        this.leaderElected = true;

        updateHeartbeat();
        executor.submit(() -> monitorHeartbeat(config.electionHeartbeatTimeoutMs()));

    }

    public synchronized void updateHeartbeat() {
        lastHeartbeat = System.currentTimeMillis();
    }

    public void setLeaderElected(boolean leaderElected) {
        this.leaderElected = leaderElected;
    }


    public void monitorHeartbeat(long timeoutMs) {
        while (running) {
            if (leaderElected && (getBroker().getId() != getBroker().getLeader()) && (System.currentTimeMillis() - lastHeartbeat > timeoutMs)) {
                leaderElected = false;
                broker.initiateElection();
            }

            try {
                Thread.sleep(timeoutMs / 2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

        }
    }

    @Override
    public void run() {
        while(running){
            try {
                Socket clientSocket = serverSocket.accept();
                ReceiverHandler handler = new ReceiverHandler(this, clientSocket);
                executor.submit(handler);
            } catch (IOException e) {
                if (running){
                    System.err.println("error accepting client connection: " + e.getMessage());
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public Broker getBroker() {
        return broker;
    }

    @Override
    public void shutdown() {
        running = false;

        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException e) {
            System.err.println("error closing server socket: " + e.getMessage());
            throw new RuntimeException(e);
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }
}
