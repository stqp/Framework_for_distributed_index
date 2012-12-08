package util;
import main.Main;

// AlphanumericID.java

// immutable
public final class AlphanumericID implements ID, Comparable<ID> {
	private final static String CHARACTERS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";
	private final static int MAX_LENGTH = 64;

	private String value;

	public AlphanumericID() {
		this.value = "";
	}

	public AlphanumericID(String value) {
		// CHECK
		if (value != null && (value.length() > MAX_LENGTH || !value.matches("[a-zA-Z0-9_]*"))) {
			System.err.println("WARNING AlphanumericID");
		}
		// 

		this.value = (value != null) ? value : "";
	}

	public AlphanumericID toInstance(String text) {
		return AlphanumericID._toInstance(text);
	}

	public static AlphanumericID _toInstance(String text) {
		return new AlphanumericID(text);
	}

	public String toMessage() {
		return this.value;
	}


	public ID getID(String value) {
		return new AlphanumericID(value);
	}

	public AlphanumericID getRandomID() {
		return AlphanumericID._getRandomID();
	}

	public static AlphanumericID _getRandomID() {
		StringBuilder sb = new StringBuilder(MAX_LENGTH);
		for (int i = 0; i < MAX_LENGTH; i++) {
			int r = Main.random.nextInt(CHARACTERS.length());
			sb.append(CHARACTERS.charAt(r));
		}
		return new AlphanumericID(sb.toString());
	}

	public boolean equals(Object obj) {
		if (obj instanceof AlphanumericID) {
			AlphanumericID o = (AlphanumericID)obj;
			return this.value.equals(o.value);
		}
		return false;
	}

	public int hashCode() {
		return this.value.hashCode();
	}

	public int compareTo(ID id) {
		if (id instanceof AlphanumericID) {
			AlphanumericID o = (AlphanumericID)id;
			return compareTo(o);
		}
		System.err.println("WARNING AlpanumericID#compareTo");
		return -1;
	}

	private int compareTo(AlphanumericID id) {
		return this.value.compareTo(id.value);
	}

	public String toString() {
		return this.value;
	}
}
