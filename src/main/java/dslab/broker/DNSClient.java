package dslab.broker;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

public class DNSClient {
    private Socket socket;
    private PrintWriter out;
    private Scanner in;
    private final String host;
    private final int port;

    public DNSClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public boolean connect(){
        try {
            socket = new Socket(host, port);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new Scanner(socket.getInputStream());
        } catch (IOException e) {
            return false;
        }


        // wait for ok SDP
        String connectResponse = in.nextLine();
        if (!connectResponse.equalsIgnoreCase("ok SDP")){
            shutdown();
            return false;
        }


        return true;
    }

    private boolean isConnected() {
        return socket != null && !socket.isClosed();
    }

    private String getServerResponse() {
        if (in.hasNextLine()) {
            return in.nextLine();
        }
        return null;
    }


    public String resolve(String domainName){
        if (!isConnected()){
            System.out.println("error: Not connected to the server");
            return null;
        }

        out.println("resolve " + domainName);
        return getServerResponse();
    }

    public String register(String domainName, String ipPort){
        if (!isConnected()){
            System.out.println("error: Not connected to the server");
            return null;
        }

        out.println("register " + domainName + " " + ipPort);

        return getServerResponse();
    }

    public String unregister(String domainName){
        if (!isConnected()){
            System.out.println("error: Not connected to the server");
            return null;
        }

        out.println("unregister " + domainName);
        return getServerResponse();
    }

    public String exit() {
        if (!isConnected()){
            return null;
        }

        out.println("exit");
        String response = getServerResponse();
        shutdown();
        return response;
    }


    private void shutdown() {
        if(socket != null) {
            try {
                socket.close(); // input and output streams are also automatically closed with that operation
            } catch (IOException e) {
                System.err.println("error when closing socket");
            }
        }
    }

}
