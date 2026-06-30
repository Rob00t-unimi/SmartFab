package dps.peer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class ProductionLineNodeTest {

    @Test
    public void testValidArgumentsWithDefaultServerUrl() {
        String[] args = {"1", "127.0.0.1", "5001"};
        ProductionLineNode.NodeConfig config = ProductionLineNode.parseArgs(args);

        assertNotNull(config);
        assertEquals(1, config.self().id());
        assertEquals("127.0.0.1", config.self().ip());
        assertEquals(5001, config.self().port());
        assertEquals("http://localhost:8080", config.serverUrl());
    }

    @Test
    public void testValidArgumentsWithCustomServerUrl() {
        String[] args = {"2", "localhost", "5002", "http://192.168.1.100:9000"};
        ProductionLineNode.NodeConfig config = ProductionLineNode.parseArgs(args);

        assertNotNull(config);
        assertEquals(2, config.self().id());
        assertEquals("localhost", config.self().ip());
        assertEquals(5002, config.self().port());
        assertEquals("http://192.168.1.100:9000", config.serverUrl());
    }

    @Test
    public void testInsufficientArguments() {
        String[] args = {"1", "127.0.0.1"};
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            ProductionLineNode.parseArgs(args);
        });
        assertTrue(exception.getMessage().contains("Insufficient arguments"));
    }

    @Test
    public void testInvalidIdFormat() {
        String[] args = {"abc", "127.0.0.1", "5001"};
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            ProductionLineNode.parseArgs(args);
        });
        assertTrue(exception.getMessage().contains("ID must be an integer"));
    }

    @Test
    public void testInvalidPortFormat() {
        String[] args = {"1", "127.0.0.1", "xyz"};
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            ProductionLineNode.parseArgs(args);
        });
        assertTrue(exception.getMessage().contains("Port must be an integer"));
    }

    @Test
    public void testNegativeId() {
        String[] args = {"-1", "127.0.0.1", "5001"};
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            ProductionLineNode.parseArgs(args);
        });
        assertTrue(exception.getMessage().contains("ID must be non-negative"));
    }

    @Test
    public void testInvalidIpAddress() {
        String[] args = {"1", "999.999.999.999", "5001"};
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            ProductionLineNode.parseArgs(args);
        });
        assertTrue(exception.getMessage().contains("Invalid IP address format"));
    }

    @Test
    public void testInvalidPortRange() {
        String[] args = {"1", "127.0.0.1", "80"};
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            ProductionLineNode.parseArgs(args);
        });
        assertTrue(exception.getMessage().contains("Port must be between 1024 and 65535"));
    }
}
