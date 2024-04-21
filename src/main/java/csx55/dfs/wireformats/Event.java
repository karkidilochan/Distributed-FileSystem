package csx55.dfs.wireformats;

import java.io.IOException;

/* Interface with method definitions that will be implemented by all wireformats */
public interface Event {
    public int getType();

    public byte[] getBytes() throws IOException;

}
