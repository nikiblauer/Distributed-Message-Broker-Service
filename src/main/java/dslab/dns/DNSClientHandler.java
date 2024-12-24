package dslab.dns;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Arrays;
import java.util.Map;
import java.util.Scanner;

public class DNSClientHandler implements Runnable {
    private final Socket clientSocket;
    private final PrintWriter out;
    private final Scanner scanner;
    private boolean running;
    private final Map<String, String> sharedData;

    public DNSClientHandler(Map<Thread, DNSClientHandler> threadMap, Socket clientSocket, Map<String, String> sharedData) {
        threadMap.put(Thread.currentThread(), this);

        this.clientSocket = clientSocket;
        this.sharedData = sharedData;

        try {
            out = new PrintWriter(clientSocket.getOutputStream(), true);
            scanner = new Scanner(clientSocket.getInputStream());
        } catch (IOException e) {
            System.err.println("error when opening streams of socket");
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        running = true;
        out.println("ok SDP");

        while(running) {
            if (!scanner.hasNextLine()) {
                break;
            }
            String input = scanner.nextLine();

            String[] tokens = input.split(" ");
            String command = tokens[0].toLowerCase();
            String[] args = Arrays.copyOfRange(tokens, 1, tokens.length);

            switch (command){
                case "exit":{
                    handleExit();
                    break;
                }
                case "resolve": {
                    if (!handleResolve(args)){
                        handleExit();
                    }
                    break;
                }
                case "register": {
                    if (!handleRegister(args)){
                        handleExit();
                    }
                    break;
                }
                case "unregister": {
                    if(!handleUnregister(args)){
                        handleExit();
                    }
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

    private boolean handleResolve(String[] args) {
        if (args.length != 1) {
            out.println("error usage: resolve <name>");
            return false;
        }

        String domainDame = args[0];
        String ipPort = sharedData.get(domainDame);
        if(ipPort == null) {
            out.println("error domain not found");
            return false;
        }

        out.println(ipPort);
        return true;
    }

    private boolean handleRegister(String[] args) {
        if (args.length != 2) {
            out.println("error usage: register <name> <ip:port>");
            return false;
        }

        String domainName = args[0];
        String ipPort = args[1];
        sharedData.put(domainName, ipPort);

        out.println("ok");
        return true;
    }

    private boolean handleUnregister(String[] args) {
        if (args.length != 1) {
            out.println("error usage: unregister <name>");
            return false;
        }

        String domainName = args[0];
        sharedData.remove(domainName);

        out.println("ok");
        return true;
    }


    public void shutdown() {
        running = false;
        if(clientSocket != null && !clientSocket.isClosed()) {
            try {
                clientSocket.close(); // input and output streams are also automatically closed with that operation
            } catch (IOException e) {
                System.err.println("error when closing socket");
            }
        }
    }
}
