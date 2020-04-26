package test.server.pojos;

import distributed.server.pojos.AtomicFloat;
import distributed.server.pojos.ProposedValue;
import distributed.server.pojos.Server;
import distributed.server.pojos.WeightedResponse;
import org.junit.Test;
import static org.junit.Assert.*;

public class PojoMutators {

    // Test Value mutators
    @Test public void mt0() {
        ProposedValue value = new ProposedValue();
        value.setValue("1");
        value.setId(1);

        assertEquals(value.getValue(), "1");
        assertEquals(value.getId(), 1);

        assertNotEquals(value.getValue(), "2");
        assertNotEquals(value.getId(), 2);
    }

    // Test WeightedResponse mutators
    @Test public void mt2() {
        WeightedResponse response = new WeightedResponse("", (float)0.2);
        response.setResponse("test");
        response.setWeight((float)0.1);

        assertEquals(response.getResponse(), "test");
        assertTrue(response.getWeight() == (float)0.1);

        assertNotEquals(response.getResponse(), "");
        assertNotEquals(response.getWeight(), 0.2);
    }

    // Test Server mutators
    @Test public void mt3() {
        Server server = new Server();
        server.setIpAddress("192.9.200.32");
        server.setPort(80);
        server.setServerId(1);
        server.setWeight((float)0.1);

        assertEquals(server.getIpAddress(), "192.9.200.32");
        assertEquals(80, (int) server.getPort());
        assertEquals(1, (int) server.getServerId());
        assertEquals((float)0.1, server.getWeight(), 0.0);

        assertNotEquals(server.getIpAddress(), "192.9.200.110");
        assertTrue(server.getPort() != 90);
        assertTrue(server.getServerId() != 2);
        assertTrue(server.getWeight() != (float)0.2);
    }

    // Test AtomicFloat mutators
    @Test public void mt4() {
        AtomicFloat fl = new AtomicFloat((float)63.3);

        assertEquals(fl.get(), (float) 63.3, 0.0);

        fl.set((float)78.9);

        assertEquals(fl.get(), (float) 78.9, 0.0);
        assertTrue(fl.get() != (float)63.3);

        assertEquals(fl.getAndSet((float) 33.4), (float) 78.9, 0.0);
        assertTrue(fl.get() == (float)33.4);
    }

}
