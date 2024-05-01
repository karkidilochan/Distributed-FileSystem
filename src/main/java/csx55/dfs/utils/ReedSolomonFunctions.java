package csx55.dfs.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import csx55.dfs.erasure.ReedSolomon;

public class ReedSolomonFunctions {
    private final String DATA_DIRECTORY = System.getProperty("user.home") + "/Documents/cs555/distributed-fs/data/";

    public static final int DATA_SHARDS = 6;

    public static final int PARITY_SHARDS = 3;
    public static final int TOTAL_SHARDS = 9;

    public static final int BYTES_IN_INT = 4;

    public static byte[][] encodeFile(String[] arguments) throws IOException {

        // Parse the command line

        final File inputFile = new File(arguments[0]);

        // Get the size of the input file. (Files bigger that
        // Integer.MAX_VALUE will fail here!)
        final int fileSize = (int) inputFile.length();

        // Figure out how big each shard will be. The total size stored
        // will be the file size (8 bytes) plus the file.
        final int storedSize = fileSize + BYTES_IN_INT;
        final int shardSize = (storedSize + DATA_SHARDS - 1) / DATA_SHARDS;

        // Create a buffer holding the file size, followed by
        // the contents of the file.
        final int bufferSize = shardSize * DATA_SHARDS;
        final byte[] allBytes = new byte[bufferSize];
        ByteBuffer.wrap(allBytes).putInt(fileSize);
        InputStream in = new FileInputStream(inputFile);
        int bytesRead = in.read(allBytes, BYTES_IN_INT, fileSize);

        in.close();

        // Make the buffers to hold the shards.
        byte[][] shards = new byte[TOTAL_SHARDS][shardSize];

        // Fill in the data shards
        for (int i = 0; i < DATA_SHARDS; i++) {
            System.arraycopy(allBytes, i * shardSize, shards[i], 0, shardSize);
        }

        // Use Reed-Solomon to calculate the parity.
        ReedSolomon reedSolomon = ReedSolomon.create(DATA_SHARDS, PARITY_SHARDS);
        reedSolomon.encodeParity(shards, 0, shardSize);

        // Write out the resulting files.
        return shards;
        // for (int i = 0; i < TOTAL_SHARDS; i++) {
        // File outputFile = new File(
        // inputFile.getParentFile(),
        // inputFile.getName() + "." + i);
        // OutputStream out = new FileOutputStream(outputFile);
        // out.write(shards[i]);
        // out.close();
        // System.out.println("wrote " + outputFile);
        // }
    }

    public static byte[] decodeFile(List<byte[]> shardsList) throws Exception {

        // Read in any of the shards that are present.
        // (There should be checking here to make sure the input
        // shards are the same size, but there isn't.)
        final byte[][] shards = new byte[TOTAL_SHARDS][];

        final boolean[] shardPresent = new boolean[TOTAL_SHARDS];
        int shardSize = 0;
        int shardCount = 0;
        for (int i = 0; i < TOTAL_SHARDS; i++) {
            shards[i] = shardsList.get(i);
            shardSize = (int) shardsList.get(i).length;
            System.out.println(shardSize);
            shardPresent[i] = true;
            shardCount += 1;

        }

        // We need at least DATA_SHARDS to be able to reconstruct the file.
        if (shardCount < DATA_SHARDS) {
            throw new Exception("Not enough shards present");

        }

        // Make empty buffers for the missing shards.
        for (int i = 0; i < TOTAL_SHARDS; i++) {
            if (!shardPresent[i]) {
                shards[i] = new byte[shardSize];
            }
        }

        // Use Reed-Solomon to fill in the missing shards
        ReedSolomon reedSolomon = ReedSolomon.create(DATA_SHARDS, PARITY_SHARDS);
        reedSolomon.decodeMissing(shards, shardPresent, 0, shardSize);

        // Combine the data shards into one buffer for convenience.
        // (This is not efficient, but it is convenient.)
        byte[] allBytes = new byte[shardSize * DATA_SHARDS];
        for (int i = 0; i < DATA_SHARDS; i++) {
            System.arraycopy(shards[i], 0, allBytes, shardSize * i, shardSize);
        }

        return allBytes;

        // System.out.println(allBytes);

        // // Extract the file length
        // int fileSize = ByteBuffer.wrap(allBytes).getInt();

        // // Write the decoded file
        // File decodedFile = new File("demo_decoded.txt");
        // OutputStream out = new FileOutputStream(decodedFile);
        // out.write(allBytes, BYTES_IN_INT, fileSize);
        // System.out.println("Wrote " + decodedFile);
    }

    /* this will take each chunk byte array and return encode total shard */
    public static byte[][] encode(byte[] chunk) {

        System.out.println("Starting encoding");
        System.out.println(chunk);

        int fileSize = (int) chunk.length;

        int storedSize = fileSize + BYTES_IN_INT;
        int shardSize = (storedSize + DATA_SHARDS - 1) / DATA_SHARDS;

        int bufferSize = shardSize * DATA_SHARDS;
        byte[] allBytes = new byte[bufferSize];
        ByteBuffer.wrap(allBytes).putInt(fileSize);

        System.arraycopy(chunk, 0, allBytes, BYTES_IN_INT, chunk.length);

        byte[][] shards = new byte[TOTAL_SHARDS][shardSize];

        for (int i = 0; i < DATA_SHARDS; i++) {
            System.arraycopy(allBytes, i * shardSize, shards[i], 0, shardSize);
        }

        ReedSolomon reedSolomon = ReedSolomon.create(DATA_SHARDS,
                PARITY_SHARDS);
        System.out.println("now encoding");
        reedSolomon.encodeParity(shards, 0, shardSize);

        // finally store the contents of the 'shards' 2-D array
        return shards;

    }

    public static byte[] decode(byte[][] shards) {

        System.out.println("decoding shards");

        // Read in any of the shards that are present.
        // (There should be checking here to make sure the input
        // shards are the same size, but there isn't.)
        // byte[][] shards = new byte[TOTAL_SHARDS][];
        boolean[] shardPresent = new boolean[TOTAL_SHARDS];
        int shardSize = 0;
        int shardCount = 0;

        System.out.println(shards.length);

        // now read the shards from the persistance store
        for (int i = 0; i < TOTAL_SHARDS; i++) {
            // Check if the shard is available.
            // If avaialbe, read its content into shards[i]
            // set shardPresent[i] = true and increase the shardCount by 1.
            // if (shardMap.containsKey(i)) {
            shardPresent[i] = true;
            // shards[i] = shardMap.get(i);
            // shardCount += 1;
            // }
            // shards[i] = shardMap[i];
            // shardPresent[i] = true;
            // shardCount += 1;
        }

        // We need at least DATA_SHARDS to be able to reconstruct the file.
        if (shardCount < DATA_SHARDS) {
            return new byte[0];
        }

        // Make empty buffers for the missing shards.
        for (int i = 0; i < TOTAL_SHARDS; i++) {
            // if (!shardPresent[i]) {
            shards[i] = new byte[shardSize];
            // }
        }

        // Use Reed-Solomon to fill in the missing shards
        ReedSolomon reedSolomon = ReedSolomon.create(DATA_SHARDS,
                PARITY_SHARDS);
        reedSolomon.decodeMissing(shards, shardPresent, 0, shardSize);

        byte[] allBytes = new byte[shardSize * DATA_SHARDS];
        for (int i = 0; i < DATA_SHARDS; i++) {
            System.arraycopy(shards[i], 0, allBytes, shardSize * i, shardSize);
        }

        return allBytes;
    }

    // public static byte[][] encode(byte[] chunk) {

    // System.out.println("Starting encoding");
    // System.out.println(chunk);
    // return new byte[0][0];

    // byte[] chunkData = chunk;
    // int storedsize = chunkData.length + BYTES_IN_INT;
    // int shardsize = (storedsize + DATA_SHARDS - 1) / DATA_SHARDS;
    // int buffersize = shardsize * DATA_SHARDS;
    // byte[] allBytes = new byte[buffersize];
    // ByteBuffer.wrap(allBytes).putInt(chunkData.length);
    // System.arraycopy(chunkData, 0, allBytes, BYTES_IN_INT, chunkData.length);

    // int paddingLen = buffersize - (storedsize);
    // byte[] paddedZeros = new byte[paddingLen];
    // for (int j = 0; j < paddingLen; j++) {
    // paddedZeros[j] = 0;
    // }
    // if (paddingLen != 0) {
    // System.arraycopy(paddedZeros, 0, allBytes, storedsize, paddingLen);
    // }
    // byte[][] shards = new byte[TOTAL_SHARDS][shardsize];

    // for (int j = 0; j < DATA_SHARDS; j++) {
    // System.arraycopy(allBytes, j * shardsize, shards[j], 0, shardsize);
    // }

    // /* this is the original data */

    // // file size
    // // int fileSize = (int) inputFile.length();

    // // total size of the stored data = length of the payload size
    // int storedSize = chunk.length + BYTES_IN_INT;

    // // size of a shard. Make sure all the shards are of the same size.
    // // In order to do this, you can padd 0s at the end.
    // // This particular code works for 4 data shards.
    // // Based on the number of shards, use a appropriate way to
    // // decide on shard size.
    // int shardSize = (storedSize + DATA_SHARDS - 1) / DATA_SHARDS;

    // // Create a buffer holding the file size, followed by the contents of the
    // file
    // // (and padding if required)
    // int bufferSize = shardSize * DATA_SHARDS;

    // /* this is the place to copy the chunk into */
    // byte[] allBytes = new byte[bufferSize];

    // /*
    // * You should implement the code for copying the file size, payload and
    // * padding into the byte array in here.
    // */
    // // ByteBuffer.wrap(allBytes).putInt(chunk.length);
    // // System.arraycopy(chunk, 0, allBytes, BYTES_IN_INT, chunk.length);

    // byte[] buffer = ByteBuffer.allocate(bufferSize).put(chunk).array();

    // // Make the buffers to hold the shards.
    // byte[][] shards = new byte[TOTAL_SHARDS][shardSize];

    // // Fill in the data shards
    // for (int i = 0; i < DATA_SHARDS; i++) {
    // System.arraycopy(buffer, i * shardSize, shards[i], 0, shardSize);
    // }

    // Use Reed-Solomon to calculate the parity. Parity codes
    // will be stored in the last two positions in 'shards' 2-D array.
    // ReedSolomon reedSolomon = new ReedSolomon(DATA_SHARDS,
    // PARITY_SHARDS);
    // reedSolomon.encodeParity(shards, 0, shardsize);

    // finally store the contents of the 'shards' 2-D array
    // return shards;

    // }
}
