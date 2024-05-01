package csx55.dfs.erasure;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Shard {
    public int sequenceNumber;
    public String filePath;

    public Shard(int sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public boolean writeShard(String shardPathPrefix, String chunkName, int sequence, byte[] shard) {
        /*
         * first create the filename of the chunk with sequence number like this
         * /SimFile.data_chunk2
         * write it to the chunk directory
         * keep track of it in the chunks list and new chunks list for the heartbeat
         * then, create 8KB slices of the chunk and generate sha-1 checksum for each
         * add those hashes into the list of the chunk object
         */
        String uploadPath = shardPathPrefix + chunkName + "_shard" + sequence;
        this.filePath = uploadPath;
        File shardFile = new File(uploadPath);

        try {
            shardFile.getParentFile().mkdirs();
            Path filePath = Paths.get(uploadPath);
            if (!Files.exists(filePath)) {
                // Create a new file
                Files.createFile(filePath);
            }
            Files.write(filePath, shard);
            System.out.println("Successfully wrote shard: " + uploadPath);

            return true;
        } catch (IOException e) {
            System.out.println("Error occurred while trying to write shard: " + uploadPath +
                    e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
}
