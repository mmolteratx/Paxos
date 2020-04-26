package test.server.pojos;

import distributed.server.pojos.ProposedValue;
import distributed.server.pojos.SafeValue;
import distributed.server.pojos.Value;
import org.junit.Test;
import static org.junit.Assert.*;

public class ValueTests {

    // Initialized with no value should not be equal
    @Test public void vt0() {
        Value value1 = new SafeValue();
        Value value2 = new SafeValue();

        assertFalse(value1.equals(value2));
    }
    // Unless, you are comparing an object to itself
    @Test public void vt1() {
        Value value1 = new SafeValue();

        assertTrue(value1.equals(value1));
    }

    // Object should equal itself
    @Test public void vt2() {
        Value value1 = new SafeValue();

        assertTrue(value1.equals(value1));

    }

    // Initialized with same value should be equal
    @Test public void vt3() {
        Value value1 = new SafeValue();
        Value value2 = new SafeValue();
        value1.setValue("1");
        value2.setValue("1");

        assertTrue(value1.equals(value2));
        assertTrue(value2.equals(value1));

    }

    // Initialized with different value should be equal, even with same ID
    @Test public void vt4() {
        Value value1 = new SafeValue();
        Value value2 = new SafeValue();
        value1.setValue("1");
        value1.setValue("2");

        assertFalse(value1.equals(value2));
        assertFalse(value2.equals(value1));
    }

    // Should be able to compare a proposed value to a safe value - equal
    @Test public void vt5() {
        Value value1 = new SafeValue();
        Value value2 = new ProposedValue();
        value1.setValue("1");
        value2.setValue("1");

        assertTrue(value1.equals(value2));
    }

    // Should be able to compare a proposed value to a safe value - not equal
    @Test public void vt6() {
        Value value1 = new SafeValue();
        Value value2 = new ProposedValue();
        value1.setValue("1");
        value2.setValue("2");

        assertFalse(value1.equals(value2));
    }

    // Should not be able to compare to an arbitrary object that is not an instance of value
    @Test public void vt7() {
        Value value1 = new SafeValue();
        Object value2 = new Object();

        assertFalse(value1.equals(value2));
        assertFalse(value2.equals(value1));
    }

    // Same value, different ID
    @Test public void vt8() {
        Value value1 = new SafeValue();
        Value value2 = new SafeValue();

        value1.setValue("1");
        value2.setValue("2");

        value1.setId(1);
        value2.setId(2);

        assertFalse(value1.equals(value2));
        assertFalse(value2.equals(value1));
    }

    // Hashcodes should be equal if values/IDs equal
    @Test public void vt9() {
        Value value1 = new SafeValue();
        Value value2 = new SafeValue();

        value1.setValue("1");
        value2.setValue("1");

        assertEquals(value1.hashCode(), value2.hashCode());

        Value value3 = new ProposedValue();

        value3.setValue("1");

        assertEquals(value1.hashCode(), value3.hashCode());
    }

    // Hashcodes should not be equal otherwise
    @Test public void vt10() {
        Value value1 = new SafeValue();
        Value value2 = new SafeValue();

        value1.setValue("1");
        value2.setValue("2");

        assertNotEquals(value1.hashCode(), value2.hashCode());

        value2.setValue("1");
        value2.setId(2);

        assertNotEquals(value1.hashCode(), value2.hashCode());
    }
}
