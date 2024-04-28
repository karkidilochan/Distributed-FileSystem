package csx55.dfs.wireformats;

/**
 * Protocol interface defines the protocol constants used for communication
 * between nodes.
 */
public interface Protocol {

    final int CLIENT_REGISTER_REQUEST = 0;
    final int REGISTER_RESPONSE = 1;
    final int DEREGISTER_REQUEST = 2;
    final int MAJOR_HEARTBEAT = 3;
    final int MINOR_HEARTBEAT = 4;

    final int CHUNK_TRANSFER = 5;
    final int CHUNK_TRANSFER_RESPONSE = 6;
    final int CHUNK_SERVER_REGISTER_REQUEST = 8;
    final int FETCH_CHUNK_SERVERS = 9;
    final int CHUNK_SERVER_LIST = 10;

    final byte SUCCESS = (byte) 200;
    final byte FAILURE = (byte) 500;
}
