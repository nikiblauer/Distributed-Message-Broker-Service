package dslab.broker;

import dslab.ComponentFactory;
import dslab.broker.enums.ElectionType;
import dslab.broker.enums.ElectionState;
import dslab.config.BrokerConfig;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.*;

public class Broker implements IBroker {

    private final BrokerConfig config;
    private final ServerSocket serverSocket;
    private final ExecutorService executor;
    private volatile boolean running;
    private final MonitoringClient monitoringClient;
    private final Map<Thread, BrokerClientHandler> threadMap;
    private final Map<String, Exchange> exchanges;
    private final Map<String, Queue> queues;

    // Leader Election
    private volatile ElectionState electionState;
    private volatile boolean heartbeatReceived;
    private final ElectionType electionType;
    private volatile int leader;

    private final Sender sender;
    private final Receiver receiver;
    private final ScheduledExecutorService scheduler;


    public Broker(BrokerConfig config) {
        this.executor = Executors.newVirtualThreadPerTaskExecutor();

        this.config = config;

        registerDomain(config.domain());
        this.monitoringClient = new MonitoringClient(config.monitoringHost(), config.monitoringPort(), config.host(), config.port());

        try {
            this.serverSocket = new ServerSocket(config.port());
        } catch (IOException e) {
            System.err.println("error creating server socket: " + e.getMessage());
            throw new RuntimeException(e);
        }

        exchanges = new ConcurrentHashMap<>();
        queues = new ConcurrentHashMap<>();
        threadMap = new ConcurrentHashMap<>();

        Exchange defaultExchange = new Exchange(ExchangeType.DEFAULT, "default");
        this.exchanges.put("default", defaultExchange);


        // LeaderElection
        this.scheduler = Executors.newScheduledThreadPool(
                0, // No core threads
                Thread.ofVirtual().factory() // Virtual thread factory
        );
        this.leader = -1;
        this.electionType = ElectionType.valueOf(this.config.electionType().toUpperCase());
        this.sender = new Sender(this);
        this.receiver = new Receiver(this);
        this.electionState = ElectionState.FOLLOWER;
        this.heartbeatReceived = false;
        startElectionHandling();
    }


    public void startElectionHandling() {
        receiver.start(); // Start listening for incoming messages

        // Schedule tasks for monitoring heartbeats
        scheduler.scheduleAtFixedRate(this::monitorHeartbeat, 0, config.electionHeartbeatTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    private void monitorHeartbeat() {
        if ((electionState == ElectionState.FOLLOWER) && !heartbeatReceived) {
            //System.out.println("Node " + getId() + " detected leader failure (Timeout: " + config.electionHeartbeatTimeoutMs() + "ms)");
            initiateElection();
        }
        heartbeatReceived = false;
    }

    public BrokerConfig getConfig() {
        return config;
    }

    @Override
    public void run() {
        this.running = true;

        while(running){
            try {
                Socket clientSocket = serverSocket.accept();
                BrokerClientHandler handler = new BrokerClientHandler(monitoringClient, threadMap, clientSocket, exchanges, queues);
                executor.submit(handler);
            } catch (IOException e) {
                if (running){
                    //System.err.println("error accepting client connection: " + e.getMessage());
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void registerDomain(String domain){
        DNSClient client = new DNSClient(config.dnsHost(), config.dnsPort());
        if (client.connect()){
            client.register(domain, config.host() + ':' + config.port());
            client.exit();
        }
    }

    @Override
    public int getId() {
        return config.electionId();
    }


    public void handleMessage(String message) {
        if (message.startsWith("ping")) {
            heartbeatReceived = true;
        } else if (message.startsWith("elect")) {
            electionState = ElectionState.CANDIDATE;
            int candidateId = Integer.parseInt(message.split(" ")[1]);
            if (candidateId < getId()) {
                //System.out.println("Node " + getId() + " is replacing candidate " + candidateId + " with its own ID in election");
                sender.sendMessage("elect " + getId());
            } else if (candidateId > getId()) {
                sender.sendMessage(message);
            } else if (candidateId == getId()) {
                leader = getId();
                electionState = ElectionState.LEADER;
                //System.out.println("Node " + getId() + " is the new leader");

                sender.sendMessage("declare " + getId());
                sender.establishConnectionsForLeader(); // Establish persistent connections
                registerDomain(config.electionDomain());
            }
        } else if (message.startsWith("declare")) {
            int leaderId = Integer.parseInt(message.split(" ")[1]);
            if (leaderId == getId()) {
                electionState = ElectionState.LEADER;
                //System.out.println("Node " + getId() + " acknowledges it is the leader");
            } else {
                electionState = ElectionState.FOLLOWER;
                leader = leaderId;

                sender.closeConnections(); // Stop persistent connections if no longer leader
                //System.out.println("Node " + getId() + " recognizes Node " + leaderId + " as leader");
                sender.sendMessage(message);

            }
        }
    }

    @Override
    public void initiateElection() {
        electionState = ElectionState.CANDIDATE;
        if(!sender.sendMessage("elect " + getId())){
            leader = getId();
            electionState = ElectionState.LEADER;
        }
    }

    @Override
    public int getLeader() {
        return leader;
    }



    @Override
    public void shutdown() {
        running = false;
        scheduler.shutdown();
        receiver.shutdown();
        sender.shutdown();


        monitoringClient.shutdown();



        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException e) {
            System.err.println("error closing server socket: " + e.getMessage());
            throw new RuntimeException(e);
        }

        for (BrokerClientHandler handler : threadMap.values()) {
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
        ComponentFactory.createBroker(args[0]).run();
    }
}