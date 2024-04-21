package csx55.dfs.wireformats;

/**
 * Protocol interface defines the protocol constants used for communication
 * between nodes.
 */
public interface Protocol {

    final int REGISTER_REQUEST = 0;
    final int REGISTER_RESPONSE = 1;
    final int DEREGISTER_REQUEST = 2;
    final int MAJOR_HEARTBEAT = 3;
    final int MINOR_HEARTBEAT = 4;

    final byte SUCCESS = (byte) 200;
    final byte FAILURE = (byte) 500;
}
