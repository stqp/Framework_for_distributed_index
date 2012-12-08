package utilTest;

import static org.junit.Assert.*;

import org.junit.Test;

import util.AlphanumericID;

public class AlphanumericIDTest {

	@Test
	public void testConstructor() {
		String value = "a123456789";
		AlphanumericID id = new AlphanumericID(value);
		assertEquals(value, id.toMessage());
		
		value = null;
		id = new AlphanumericID(value);
		assertEquals("", id.toMessage());
	}


}
