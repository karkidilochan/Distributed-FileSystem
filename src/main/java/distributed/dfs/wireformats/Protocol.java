package distributed.dfs.wireformats;

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
    final int CREATE_REPLICA = 11;
    final int CREATE_REPLICA_RESPONSE = 12;
    final int FETCH_CHUNKS = 13;
    final int FETCH_CHUNKS_RESPONSE = 14;

    final int REQUEST_CHUNK = 15;
    final int REQUEST_CHUNK_RESPONSE = 16;
    final int REPORT_CHUNK_CORRUPTION = 17;
    final int ERROR_CORRECTION = 18;
    final int CHUNK_CORRECTION = 19;
    final int REPLICATE_NEW_SERVER = 20;
    final int MIGRATE_CHUNK = 21;
    final int MIGRATE_CHUNK_RESPONSE = 22;

    final byte SUCCESS = (byte) 200;
    final byte FAILURE = (byte) 500;
}
