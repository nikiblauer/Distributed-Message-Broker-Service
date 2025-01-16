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

    private Sender sender;
    private Receiver receiver;
    private ScheduledExecutorService scheduler;


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
        this.electionType = ElectionType.valueOf(this.config.electionType().toUpperCase());

        if (electionType != ElectionType.NONE) {
            this.scheduler = Executors.newScheduledThreadPool(
                    0,
                    Thread.ofVirtual().factory()
            );
            this.leader = -1;
            this.currentVote = -1;
            this.electionState = ElectionState.FOLLOWER;
            this.sender = new Sender(this);
            this.receiver = new Receiver(this);
            this.heartbeatReceived = true;
            startElectionHandling();
        }

    }


    private void startElectionHandling() {
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
                    System.err.println("error accepting client connection: " + e.getMessage());
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
        if(electionType == ElectionType.NONE)
            return;

        if (message.startsWith("elect")) {
            handleElect(message);
        } else if (message.startsWith("declare")) {
            handleDeclare(message);
        }
    }

    private void handleElect(String message) {
        electionState = ElectionState.CANDIDATE;
        sender.closeConnections();

        if (electionType == ElectionType.RAFT) {
            return;
        }

        int candidateId = parseCandidateId(message);
        if (candidateId < getId()) {
            handleLowerCandidateId();
        } else if (candidateId > getId()) {
            handleHigherCandidateId(message);
        } else {
            // candidateId == getId()
            becomeLeader();
        }
    }

    private int parseCandidateId(String message) {
        return Integer.parseInt(message.split(" ")[1]);
    }

    private void handleLowerCandidateId() {
        if (electionType != ElectionType.BULLY) {
            // For non-Bully algorithms, simply send out our own "elect"
            sender.sendMessage("elect " + getId());
        } else {
            // Bully algorithm: If our own "elect" response count is 0, we are the leader
            if (sender.sendMessage("elect " + getId()) == 0) {
                becomeLeader();
            }
        }
    }

    private void handleHigherCandidateId(String originalMessage) {
        // Forward the election message as we found someone with a higher ID
        sender.sendMessage(originalMessage);
    }



    private void handleDeclare(String message) {
        sender.closeConnections();  // Stop persistent connections if no longer the leader

        int leaderId = parseLeaderId(message);
        if (electionType == ElectionType.RING) {
            handleDeclareRing(leaderId, message);
        } else {
            electionState = ElectionState.FOLLOWER;
            leader = leaderId;
            currentVote = -1;
        }
    }

    private int parseLeaderId(String message) {
        return Integer.parseInt(message.split(" ")[1]);
    }

    private void handleDeclareRing(int leaderId, String originalMessage) {
        if (leaderId == getId()) {
            // If the declared leader is myself, transition to LEADER
            electionState = ElectionState.LEADER;
        } else {
            // Otherwise, set ourselves to FOLLOWER and forward the message
            electionState = ElectionState.FOLLOWER;
            leader = leaderId;
            sender.sendMessage(originalMessage);
        }
    }


    @Override
    public void initiateElection() {
        if(electionType == ElectionType.NONE)
            return;

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
        if (electionType != ElectionType.NONE){
            scheduler.shutdown();
            receiver.shutdown();
            sender.shutdown();
        }


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