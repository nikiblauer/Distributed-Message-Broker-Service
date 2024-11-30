package dslab.util;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Only supports a single connection
 */
@Slf4j
public class MockServer implements Runnable {

    private static final String ERROR_RESPONSE = "INVALID MESSAGE RECEIVED";

    private ServerSocket socket;
    private final int port;
    private final BlockingQueue<String> receivedMessages = new LinkedBlockingQueue<>();

    @Setter
    private String exceptedMessage = "EXCEPTED MESSAGE NOT SET";
    @Setter
    private String response = "RESPONSE NOT SET";

    public MockServer(int port) {
        this.port = port;
    }

    @Override
    public void run() {
        try (ServerSocket socket = new ServerSocket(port)) {
            this.socket = socket;
            log.debug("Listening on port {}", port);

            while (true) {
                Socket conn = socket.accept();
                handleConnection(conn);
            }
        } catch (SocketException e) {
            log.debug("Socket closed, stopping server socket port {}", port);
            // ignore, socket closed
        } catch (IOException e) {
            log.debug("Encountered IOException, stopping server socket port {}", port);
            //
        } finally {
            shutdown();
        }
    }

    private void handleConnection(Socket conn) throws IOException {
        try (conn;
             BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
             PrintStream out = new PrintStream(conn.getOutputStream(), true)) {

            log.debug("accepted connection from {}", conn.getRemoteSocketAddress());

            out.println("ok LEP");
            while (true) {
                boolean ok = handleInput(in, out);

                // Closes the socket on exit or protocol error
                if (!ok) {
                    break;
                }
            }

        } catch (IOException e) {
            log.debug("Encountered IOException, closing connection with socket {}", conn);
        }
    }

    private boolean handleInput(BufferedReader in , PrintStream out) throws IOException {
        String read = in.readLine();

        if (read == null) return false;

        receivedMessages.add(read);

        // No answer if response is set to blank
        if (response.isBlank()) return true;

        if (exceptedMessage.equals("EXCEPTED MESSAGE NOT SET")) {
            out.println("EXCEPTED MESSAGE NOT SET, PLEASE SET A EXPECTED MESSAGE");
            return false;
        }

        if (!read.equals(exceptedMessage)) {
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
            log.warn("Unable to close socket of receiver.", e);
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
}
