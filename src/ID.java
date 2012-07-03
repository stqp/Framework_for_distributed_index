// ID.java

public interface ID {
    ID toInstance(String text);
    String toMessage();

    ID getID(String id);
    ID getRandomID();
    int compareTo(ID id);
}
