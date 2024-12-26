package dslab.broker;

import dslab.ComponentFactory;
import dslab.broker.enums.ElectionState;
import dslab.config.BrokerConfig;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
    private Receiver receiver;
    private Sender sender;
    private volatile ElectionState electionState;
    private volatile int leader;

    public Broker(BrokerConfig config) {
        this.executor = Executors.newVirtualThreadPerTaskExecutor();

        this.config = config;
        this.electionState = ElectionState.FOLLOWER;
        this.leader = -1;
        this.receiver = new Receiver(this);
        this.executor.submit(receiver);
        this.sender = new Sender(this);
        this.executor.submit(sender);


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
    }

    public ElectionState getElectionState() {
        return electionState;
    }

    public void setElectionState(ElectionState electionState) {
        this.electionState = electionState;
    }

    public BrokerConfig getConfig() {
        return config;
    }

    public Receiver getReceiver() {
        return receiver;
    }

    public Sender getSender() {
        return sender;
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

    @Override
    public void initiateElection() {
        sender.elect(this.getId());
    }

    @Override
    public int getLeader() {
        return leader;
    }

    public void setLeader(int leaderID) {
        this.leader = leaderID;

        if (leaderID == this.getId()){
            this.electionState = ElectionState.LEADER;
            registerDomain(config.electionDomain());
        }
    }



    @Override
    public void shutdown() {
        running = false;
        this.sender.shutdown();
        this.receiver.shutdown();

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
