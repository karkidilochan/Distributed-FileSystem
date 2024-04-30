package csx55.dfs.wireformats;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * WireFormatGenerator is a singleton class responsible for generating messages
 * of different types.
 */
public class WireFormatGenerator {
    private static final WireFormatGenerator messageGenerator = new WireFormatGenerator();

    // private constructor to prevent instantiation
    private WireFormatGenerator() {
    }

    /**
     * Returns the singleton instance of WireFormatGenerator.
     * 
     * @return The WireFormatGenerator instance.
     */
    public static WireFormatGenerator getInstance() {
        return messageGenerator;
    }

    /**
     * Creates a wireformat message from the received marshaled bytes.
     * 
     * @param marshalledData The marshaled bytes representing the event.
     * @return The Event object created from the marshaled bytes.
     */
    /* Create message wireformats from received marshalled bytes */
    public Event createMessage(byte[] marshalledData) throws IOException {
        int type = ByteBuffer.wrap(marshalledData).getInt();
        switch (type) {
            case Protocol.CHUNK_SERVER_REGISTER_REQUEST:
                return new Register(marshalledData);

            case Protocol.CLIENT_REGISTER_REQUEST:
                return new Register(marshalledData);

            case Protocol.REGISTER_RESPONSE:
                return new RegisterResponse(marshalledData);

            case Protocol.DEREGISTER_REQUEST:
                return new Register(marshalledData);

            case Protocol.MAJOR_HEARTBEAT:
                return new MajorHeartbeat(marshalledData);

            case Protocol.MINOR_HEARTBEAT:
                return new MinorHeartbeat(marshalledData);

            case Protocol.CHUNK_SERVER_LIST:
                return new ChunkServerList(marshalledData);

            case Protocol.CHUNK_TRANSFER:
                return new ChunkMessage(marshalledData);

            case Protocol.CHUNK_TRANSFER_RESPONSE:
                return new ChunkMessageResponse(marshalledData);

            case Protocol.FETCH_CHUNK_SERVERS:
                return new FetchChunkServers(marshalledData);

            case Protocol.CREATE_REPLICA:
                return new CreateReplica(marshalledData);

            case Protocol.CREATE_REPLICA_RESPONSE:
                return new CreateReplicaResponse(marshalledData);

            case Protocol.FETCH_CHUNKS:
                return new FetchChunksList(marshalledData);

            case Protocol.FETCH_CHUNKS_RESPONSE:
                return new FetchChunksListResponse(marshalledData);

            case Protocol.REQUEST_CHUNK:
                return new RequestChunk(marshalledData);

            case Protocol.REQUEST_CHUNK_RESPONSE:
                return new RequestChunkResponse(marshalledData);

            case Protocol.ERROR_CORRECTION:
                return new ErrorCorrection(marshalledData);

            case Protocol.REPORT_CHUNK_CORRUPTION:
                return new ReportChunkCorruption(marshalledData);

            case Protocol.CHUNK_CORRECTION:
                return new ChunkCorrection(marshalledData);

            case Protocol.REPLICATE_NEW_SERVER:
                return new ReplicateNewServer(marshalledData);

            case Protocol.MIGRATE_CHUNK:
                return new MigrateChunk(marshalledData);

            case Protocol.MIGRATE_CHUNK_RESPONSE:
                return new MigrationResponse(marshalledData);

            default:
                System.out.println("Error: WireFormat could not be generated. " + type);
                return null;
        }
    }
}
