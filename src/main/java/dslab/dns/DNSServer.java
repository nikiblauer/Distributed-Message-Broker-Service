package dslab.dns;

import dslab.ComponentFactory;
import dslab.config.DNSServerConfig;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DNSServer implements IDNSServer {
    private final ServerSocket serverSocket;
    private final ExecutorService executor;
    private boolean running;
    private final Map<String, String> sharedData;
    private final Map<Thread, DNSClientHandler> threadMap;

    public DNSServer(DNSServerConfig config) {
        try {
            serverSocket = new ServerSocket(config.port());
        } catch (IOException e) {
            System.err.println("error creating server socket: " + e.getMessage());
            throw new RuntimeException(e);
        }

        sharedData = new ConcurrentHashMap<>();
        threadMap = new ConcurrentHashMap<>();
        executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @Override
    public void run() {
        this.running = true;

        while(running){
            try {
                Socket clientSocket = serverSocket.accept();
                DNSClientHandler handler = new DNSClientHandler(threadMap, clientSocket, sharedData);
                executor.submit(handler);            }
            catch (IOException e) {
                if (running){
                    System.err.println("error accepting client connection: " + e.getMessage());
                    throw new RuntimeException(e);
                }
            }
        }
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

        for (DNSClientHandler handler : threadMap.values()) {
            handler.shutdown();
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

    public static void main(String[] args) {
        ComponentFactory.createDNSServer(args[0]).run();
    }
}
