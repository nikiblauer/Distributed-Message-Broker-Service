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
    private volatile int currentVote;

    private final Sender sender;
    private final Receiver receiver;
    private final ScheduledExecutorService scheduler;


    public Broker(BrokerConfig config) {
        this.config = config;
        this.executor = Executors.newVirtualThreadPerTaskExecutor();

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
                0,
                Thread.ofVirtual().factory()
        );
        this.leader = -1;
        this.currentVote = -1;
        this.electionType = ElectionType.valueOf(this.config.electionType().toUpperCase());
        this.electionState = ElectionState.FOLLOWER;

        this.sender = new Sender(this);
        this.receiver = new Receiver(this);
        this.heartbeatReceived = true;
        startElectionHandling();
    }


    public void startElectionHandling() {
        executor.submit(receiver);
        scheduler.scheduleAtFixedRate(this::monitorHeartbeat, 0, config.electionHeartbeatTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    private void monitorHeartbeat() {
        if ((electionState == ElectionState.FOLLOWER) && !heartbeatReceived) {
            initiateElection();
        }
        heartbeatReceived = false;
    }

    public BrokerConfig getConfig() {
        return config;
    }

    public void setElectionState(ElectionState electionState) {
        this.electionState = electionState;
    }

    public void vote(int id){
        this.currentVote = id;
    }

    public int getCurrentVote() {
        return currentVote;
    }

    public void updateHeartbeat() {
        heartbeatReceived = true;
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

        } else if (message.startsWith("elect")) {
            electionState = ElectionState.CANDIDATE;
            sender.closeConnections();
            if (electionType == ElectionType.RAFT){
                return;
            }


            int candidateId = Integer.parseInt(message.split(" ")[1]);
            if (candidateId < getId()) {
                if (electionType != ElectionType.BULLY) {
                    sender.sendMessage("elect " + getId());
                } else {
                    if(sender.sendMessage("elect " + getId()) == 0){

                        leader = getId();
                        electionState = ElectionState.LEADER;

                        sender.sendMessage("declare " + getId());
                        sender.establishConnectionsForLeader();
                        registerDomain(config.electionDomain());
                    }
                }
            } else if (candidateId > getId()) {
                sender.sendMessage(message);
            } else if (candidateId == getId()) {
                leader = getId();
                electionState = ElectionState.LEADER;

                sender.sendMessage("declare " + getId());
                sender.establishConnectionsForLeader();
                registerDomain(config.electionDomain());
            }
        } else if (message.startsWith("declare")) {

            sender.closeConnections(); // Stop persistent connections if no longer leader

            int leaderId = Integer.parseInt(message.split(" ")[1]);
            if (electionType == ElectionType.RING){
                if (leaderId == getId()) {
                    electionState = ElectionState.LEADER;
                } else {
                    electionState = ElectionState.FOLLOWER;
                    leader = leaderId;

                    sender.sendMessage(message);

                }
            } else {
                electionState = ElectionState.FOLLOWER;
                leader = leaderId;
                currentVote = -1;
            }
        }
    }

    @Override
    public void initiateElection() {
        electionState = ElectionState.CANDIDATE;

        // Send election request
        int votes = sender.sendMessage("elect " + getId());

        // Determine if we have enough votes (or no opposition in non-RAFT)
        boolean hasWon = (electionType == ElectionType.RAFT)
                ? (votes >= config.electionPeerIds().length / 2)
                : (votes == 0);

        // If the node wins, complete the process of becoming leader
        if (hasWon) {
            becomeLeader();
        }
    }

    private void becomeLeader() {
        leader = getId();
        electionState = ElectionState.LEADER;

        sender.sendMessage("declare " + getId());
        sender.establishConnectionsForLeader(); // Establish persistent connections

        // Additional domain registration or similar actions can go here
        registerDomain(config.electionDomain());
    }


    @Override
    public int getLeader() {
        return leader;
    }

    public ElectionType getElectionType() {
        return electionType;
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
            System.err.println(e.getMessage());
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