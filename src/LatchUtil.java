// LatchUtil.java

public final class LatchUtil {
    public static final int IS = 0;
    public static final int IX = 1;
    public static final int S = 2;
    public static final int SIX = 3;
    public static final int X = 4;
    public static final int TYPES = 5;

    public static int[] newLatch() {
	return new int[TYPES];
    }
}
