package dslab.broker;

import dslab.IServer;
import dslab.broker.enums.ElectionState;
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
    private Thread heartbeatMonitorThread;

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

        this.lastHeartbeat = System.currentTimeMillis();
        startHeartbeatMonitor();

    }


    /**
     * Starts the heartbeat monitor if the election state is FOLLOWER.
     */
    public void startHeartbeatMonitor() {
        if (heartbeatMonitorThread == null || !heartbeatMonitorThread.isAlive()) {
            heartbeatMonitorThread = new Thread(() -> {
                while (running) {
                    // Only run if the current state is FOLLOWER
                    if (broker.getElectionState() == ElectionState.FOLLOWER) {
                        if (System.currentTimeMillis() - lastHeartbeat > config.electionHeartbeatTimeoutMs()) {
                            broker.initiateElection();
                            // Reset lastHeartbeat to avoid continuous triggering
                            lastHeartbeat = System.currentTimeMillis();
                        }
                        try {
                            Thread.sleep(config.electionHeartbeatTimeoutMs() / 2); // Check periodically
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    } else {
                        // Exit the monitor loop if no longer a FOLLOWER
                        break;
                    }
                }
            });
            heartbeatMonitorThread.start();
        }
    }

    /**
     * Stops the heartbeat monitor.
     */
    public void stopHeartbeatMonitor() {
        if (heartbeatMonitorThread != null) {
            heartbeatMonitorThread.interrupt();
        }
    }

    public void resetHeartbeat() {
        lastHeartbeat = System.currentTimeMillis();
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
