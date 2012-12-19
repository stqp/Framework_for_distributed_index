package util;

import java.io.Serializable;

// ID.java

public interface ID extends Serializable{
    ID toInstance(String text);
    String toMessage();

    ID getID(String id);
    ID getRandomID();
    int compareTo(ID id);
}
