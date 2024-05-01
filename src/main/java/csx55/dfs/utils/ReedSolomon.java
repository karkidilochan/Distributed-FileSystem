package csx55.dfs.utils;

import java.nio.ByteBuffer;
import java.util.Map;

import erasure.ReedSolomon;

public class ReedSolomon {
    public static final int DATA_SHARDS = 4;

    public static final int PARITY_SHARDS = 2;
    public static final int TOTAL_SHARDS = 6;

    public static final int BYTES_IN_INT = 4;

    /* this will take each chunk byte array and return encode total shard */
    public static void encode(byte[] chunk) {
        /* this is the original data */

        // file size
        // int fileSize = (int) inputFile.length();

        // total size of the stored data = length of the payload size
        int storedSize = chunk.length + BYTES_IN_INT;

        // size of a shard. Make sure all the shards are of the same size.
        // In order to do this, you can padd 0s at the end.
        // This particular code works for 4 data shards.
        // Based on the number of shards, use a appropriate way to
        // decide on shard size.
        int shardSize = (storedSize + DATA_SHARDS - 1) / DATA_SHARDS;

        // Create a buffer holding the file size, followed by the contents of the file
        // (and padding if required)
        int bufferSize = shardSize * DATA_SHARDS;

        /* this is the place to copy the chunk into */
        byte[] allBytes = new byte[bufferSize];

        /*
         * You should implement the code for copying the file size, payload and
         * padding into the byte array in here.
         */
        ByteBuffer.wrap(allBytes).putInt(chunk.length);
        System.arraycopy(chunk, 0, allBytes, BYTES_IN_INT, chunk.length);

        // Make the buffers to hold the shards.
        byte[][] shards = new byte[TOTAL_SHARDS][shardSize];

        // Fill in the data shards
        for (int i = 0; i < DATA_SHARDS; i++) {
            System.arraycopy(allBytes, i * shardSize, shards[i], 0, shardSize);
        }

        // Use Reed-Solomon to calculate the parity. Parity codes
        // will be stored in the last two positions in 'shards' 2-D array.
        ReedSolomon reedSolomon = new ReedSolomon(DATA_SHARDS, PARITY_SHARDS);
        reedSolomon.encodeParity(shards, 0, shardSize);

        // finally store the contents of the 'shards' 2-D array
        System.out.println(shards);
    }

    public static void decode(Map<Integer, byte[]> shardMap) {

        // Read in any of the shards that are present.
        // (There should be checking here to make sure the input
        // shards are the same size, but there isn't.)
        byte[][] shards = new byte[TOTAL_SHARDS][];
        boolean[] shardPresent = new boolean[TOTAL_SHARDS];
        int shardSize = 0;
        int shardCount = 0;
        // now read the shards from the persistance store
        for (int i = 0; i < TOTAL_SHARDS; i++) {
            // Check if the shard is available.
            // If avaialbe, read its content into shards[i]
            // set shardPresent[i] = true and increase the shardCount by 1.
            if (shardMap.containsKey(i)) {
                shardPresent[i] = true;
                shards[i] = shardMap.get(i);
                shardCount += 1;
            }
        }

        // We need at least DATA_SHARDS to be able to reconstruct the file.
        if (shardCount < DATA_SHARDS) {
            return;
        }

        // Make empty buffers for the missing shards.
        for (int i = 0; i < TOTAL_SHARDS; i++) {
            if (!shardPresent[i]) {
                shards[i] = new byte[shardSize];
            }
        }

        // Use Reed-Solomon to fill in the missing shards
        ReedSolomon reedSolomon = new ReedSolomon(DATA_SHARDS, PARITY_SHARDS);
        reedSolomon.decodeMissing(shards, shardPresent, 0, shardSize);

        byte[] allBytes = new byte[shardSize * DATA_SHARDS];
        for (int i = 0; i < DATA_SHARDS; i++) {
            System.arraycopy(shards[i], 0, allBytes, shardSize * i, shardSize);
        }

        System.out.println(allBytes);
    }
}
