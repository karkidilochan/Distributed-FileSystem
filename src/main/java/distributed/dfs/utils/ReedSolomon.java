// package distributed.dfs.utils;

// // import erasure.ReedSolomon;

// public class ReedSolomon {

// /* this will take each chunk byte array and return encode total shard */
// public static void encode() {
// public final int DATA_SHARDS = 4;
// public final int PARITY_SHARDS = 2;
// public final int TOTAL_SHARDS = 6;

// public final int BYTES_IN_INT = 4;

// // file size
// int fileSize = (int) inputFile.length();

// // total size of the stored data = length of the payload size
// int storedSize = fileSize + BYTES_IN_INT;

// // size of a shard. Make sure all the shards are of the same size.
// // In order to do this, you can padd 0s at the end.
// // This particular code works for 4 data shards.
// // Based on the numer of shards, use a appropriate way to
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
// // Make the buffers to hold the shards.
// byte[][] shards = new byte[TOTAL_SHARDS][shardSize];

// // Fill in the data shards
// for (int i = 0; i < DATA_SHARDS; i++) {
// System.arraycopy(allBytes, i * shardSize, shards[i], 0, shardSize);
// }

// // Use Reed-Solomon to calculate the parity. Parity codes
// // will be stored in the last two positions in 'shards' 2-D array.
// ReedSolomon reedSolomon = new ReedSolomon(DATA_SHARDS, PARITY_SHARDS);
// reedSolomon.encodeParity(shards, 0, shardSize);

// // finally store the contents of the 'shards' 2-D array
// }

// public static void decode() {

// }
// }
