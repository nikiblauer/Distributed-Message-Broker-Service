package dslab.broker;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.*;

public class BrokerClientHandler implements Runnable {
    private final Socket clientSocket;
    private final PrintWriter out;
    private final Scanner scanner;
    private boolean running;
    private final Map<String, Exchange> exchanges;
    private final Map<String, Queue> queues;
    private Exchange currentExchange;
    private Queue currentQueue;
    private volatile boolean subscribed;

    Thread subscribedThread;

    public BrokerClientHandler(Map<Thread, BrokerClientHandler> threadMap, Socket clientSocket, Map<String, Exchange> exchanges, Map<String, Queue> queues) {
        threadMap.put(Thread.currentThread(), this);


        this.clientSocket = clientSocket;
        try {
            out = new PrintWriter(clientSocket.getOutputStream(), true);
            scanner = new Scanner(clientSocket.getInputStream());
        } catch (IOException e) {
            System.err.println("error when opening streams of socket");
            throw new RuntimeException(e);
        }

        this.exchanges = exchanges;
        this.queues = queues;
    }

    @Override
    public void run() {
        running = true;
        out.println("ok SMQP");

        while(running) {
            if (!scanner.hasNextLine()) {
                break;
            }
            String input = scanner.nextLine();

            String[] tokens = input.split(" ");
            String command = tokens[0].toLowerCase();
            String[] args = Arrays.copyOfRange(tokens, 1, tokens.length);

            if(subscribed){
                if(command.equals("stop")){
                    subscribed = false;
                }
                continue;
            }

            switch (command){
                case "exit":{
                    handleExit();
                    break;
                }
                case "exchange": {
                    handleExchange(args);
                    break;
                }
                case "queue": {
                    handleQueue(args);
                    break;
                }
                case "bind": {
                    handleBind(args);
                    break;
                }
                case "publish": {
                    handlePublish(args);
                    break;
                }
                case "subscribe": {
                    handleSubscribe(args);
                    break;
                }

            }

        }
        shutdown();
    }

    private void handleExit(){
        running = false;

        out.println("ok bye");
    }

    private void handleExchange(String[] args){
        if(args.length != 2){
            out.println("error usage: exchange <type> <name>");
            return;
        }

        String type = args[0];
        String name = args[1];

        Exchange exchange = exchanges.get(name);
        if (exchange != null){
            if(exchange.getType() != Exchange.convertType(type)){
                out.println("error exchange already exists with different type");
                return;
            }
        } else {
            exchange = new Exchange(Exchange.convertType(type), name);
            exchanges.put(name, exchange);
        }

        out.println("ok");

        currentExchange = exchange;

    }

    private void handleQueue(String[] args){
        if (args.length != 1){
            out.println("error usage: queue <name>");
            return;
        }
        out.println("ok");


        String name = args[0];
        currentQueue = queues.computeIfAbsent(name, Queue::new);
        exchanges.get("default").bindQueue(name, currentQueue);

    }

    private void handleBind(String[] args){
        if (args.length != 1){
            out.println("error usage: bind <binding-key>");
            return;
        }
        if (currentExchange == null){
            out.println("error no exchange declared");
            return;
        }
        if (currentQueue == null){
            out.println("error no queue declared");
            return;
        }
        out.println("ok");


        String bindingKey = args[0];
        currentExchange.bindQueue(bindingKey, currentQueue);


    }

    private void handlePublish(String[] args){
        if (args.length < 2){
            out.println("error usage: publish <routing-key> <message>");
            return;
        }

        if (currentExchange == null){
            out.println("error no exchange declared");
            return;
        }
        out.println("ok");


        String routingKey = args[0];
        String message = args[1];
        List<Queue> targetQueues = currentExchange.routeMessage(routingKey);

        for (Queue queue : targetQueues){
            queue.addMessage(message);
        }

    }

    private void handleSubscribe(String[] args){
        if (currentQueue == null){
            out.println("error no queue declared");
            return;
        }
        subscribed = true;
        out.println("ok");

        subscribedThread = Thread.ofVirtual().start(() -> {
            try {
                while (subscribed) {
                    String message = currentQueue.getMessage();
                    out.println(message);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

    }

    public void shutdown() {
        running = false;
        subscribed = false;

        if(clientSocket != null && !clientSocket.isClosed()) {
            try {
                clientSocket.close(); // input and output streams are also automatically closed with that operation
            } catch (IOException e) {
                System.err.println("error when closing socket");
            }
        }

    }
}
