package dslab.assignment2.dns;

import dslab.util.Constants;
import dslab.util.Global;
import dslab.util.helper.TelnetClientHelper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static dslab.util.CommandBuilder.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

public class DNSProtocolTest extends BaseSingleDNSTest {

    private TelnetClientHelper helper;

    @Override
    protected void initTelnetClientHelpers() throws IOException {
        helper = new TelnetClientHelper(Constants.LOCALHOST, config.port());
    }

    @Override
    protected void closeTelnetClientHelpers() throws IOException {
        helper.disconnect();
    }

    /**
     * All basic DNS Protocol tests.
     * These are combined in a singular test case to:
     * - Make grading easier
     * - Massively reduce computation time when executed on the github runner instance
     */
    @Test
    @Timeout(value = 26000, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void dns_protocol_tests() throws IOException {
        // beforeEach() called by JUnit
        dns_accepts_connection_successfully();
        afterEach();

        beforeEach();
        correct_protocol_identifier_successfully();
        afterEach();

        beforeEach();
        unregister_domain_successfully();
        afterEach();

        beforeEach();
        exit_successfully();
        afterEach();

        beforeEach();
        register_domain_successfully();
        afterEach();

        beforeEach();
        resolve_domain_not_found();
        afterEach();

        beforeEach();
        unregister_domain_wrong_syntax("unregister");
        afterEach();

        beforeEach();
        unregister_domain_wrong_syntax("unregister a b");
        afterEach();

        beforeEach();
        register_domain_wrong_syntax("register");
        afterEach();

        beforeEach();
        register_domain_wrong_syntax("register at");
        afterEach();

        beforeEach();
        resolve_domain_wrong_syntax("resolve");
        afterEach();

        beforeEach();
        resolve_domain_wrong_syntax("resolve a b");
        // afterEach() called by junit
    }

    /**
     * Tests if the dns server is accepting a new client connection
     * according to the protocol definition.
     */
    void dns_accepts_connection_successfully() {
        assertDoesNotThrow(() -> helper.connectAndReadResponse());
    }

    /**
     * Tests if the dns server is accepting a new client connection and sends ok SMQP back to the client
     * according to the protocol definition.
     *
     * @throws IOException Thrown by the telnet client helper if a connection is not possible.
     */
    void correct_protocol_identifier_successfully() throws IOException {
        assertEquals("ok SDP", helper.connectAndReadResponse());
    }

    void register_domain_successfully() throws IOException {
        helper.connectAndReadResponse();

        final String name = String.format("domain-%s", Global.SECURE_STRING_GENERATOR.getSecureString());
        final String host = String.format("192.168.0.1:%d", Global.SECURE_INT_GENERATOR.getInt(1, 2048));

        String response = helper.sendCommandAndReadResponse(register(name, host));

        assertEquals("ok", response);
    }

    /**
     * Tests if the dns server responds with an error message upon a wrong syntax
     *
     * @throws IOException Thrown by the telnet client helper if a connection is not possible.
     */
    void register_domain_wrong_syntax(String msg) throws IOException {
        helper.connectAndReadResponse();
        String response = helper.sendCommandAndReadResponse(msg);

        assertThat(response).startsWith("error");
        assertThat(response).contains("register <name> <ip:port>");
    }

    void unregister_domain_successfully() throws IOException {
        helper.connectAndReadResponse();
        String response = helper.sendCommandAndReadResponse(unregister("domain.at"));

        assertEquals("ok", response);
    }

    /**
     * Tests if the dns server responds with an error message upon a wrong syntax
     *
     * @throws IOException Thrown by the telnet client helper if a connection is not possible.
     */
    void unregister_domain_wrong_syntax(String msg) throws IOException {
        helper.connectAndReadResponse();
        String response = helper.sendCommandAndReadResponse(msg);

        assertThat(response).startsWith("error");
        assertThat(response).contains("unregister <name>");
    }

    void resolve_domain_not_found() throws IOException {
        helper.connectAndReadResponse();

        String response = helper.sendCommandAndReadResponse(resolve(Global.SECURE_STRING_GENERATOR.getSecureString()));

        assertThat(response).startsWith("error");
        assertThat(response).contains("domain");
    }

    void resolve_domain_wrong_syntax(String msg) throws IOException {
        helper.connectAndReadResponse();
        String response = helper.sendCommandAndReadResponse(msg);

        assertThat(response).startsWith("error");
        assertThat(response).contains("resolve <name>");
    }

    void exit_successfully() throws IOException {
        helper.connectAndReadResponse();

        assertEquals("ok bye", helper.sendCommandAndReadResponse("exit"));
        assertNull(helper.sendCommandAndReadResponse(register("dslab.at", "192.168.0.1:22")));
    }
}
