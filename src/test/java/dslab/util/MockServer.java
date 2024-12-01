package dslab.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Only supports a single connection
 */
public class MockServer implements Runnable {

    private static final String ERROR_RESPONSE = "INVALID MESSAGE RECEIVED";

    private ServerSocket socket;
    private final int port;
    private final BlockingQueue<String> receivedMessages = new LinkedBlockingQueue<>();

    private String expectedMessage = "EXPECTED MESSAGE NOT SET";
    private String response = "RESPONSE NOT SET";

    public MockServer(int port) {
        this.port = port;
    }

    @Override
    public void run() {
        try (ServerSocket socket = new ServerSocket(port)) {
            this.socket = socket;

            while (true) {
                Socket conn = socket.accept();
                handleConnection(conn);
            }
        } catch (IOException e) {
            // ignored
        } finally {
            shutdown();
        }
    }

    private void handleConnection(Socket conn) throws IOException {
        try (conn;
             BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
             PrintStream out = new PrintStream(conn.getOutputStream(), true)) {


            out.println("ok LEP");
            while (true) {
                boolean ok = handleInput(in, out);

                // Closes the socket on exit or protocol error
                if (!ok) {
                    break;
                }
            }

        } catch (IOException e) {
            // ignored
        }
    }

    private boolean handleInput(BufferedReader in , PrintStream out) throws IOException {
        String read = in.readLine();

        if (read == null) return false;

        receivedMessages.add(read);

        // No answer if response is set to blank
        if (response.isBlank()) return true;

        if (expectedMessage.equals("EXPECTED MESSAGE NOT SET")) {
            out.println("EXPECTED MESSAGE NOT SET, PLEASE SET A EXPECTED MESSAGE");
            return false;
        }

        if (!read.equals(expectedMessage)) {
            out.println(ERROR_RESPONSE);
            return false;
        }

        out.println(response);
        return true;
    }

    public void shutdown() {
        try {
            if (socket != null && !socket.isClosed()) socket.close();
        } catch (IOException e) {
            // ignored
        }
    }

    public String takeMessage() throws InterruptedException {
        return receivedMessages.take();
    }

    public String getQueue() {
        return receivedMessages.toString();
    }

    public int numberOfReceivedMsg() {
        return receivedMessages.size();
    }

    public void setExpectedMessage(String expectedMessage) {
        this.expectedMessage = expectedMessage;
    }

    public void setResponse(String response) {
        this.response = response;
    }
}
