package csx55.dfs.erasure;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;

import csx55.dfs.wireformats.ChunkMessage;

public class Chunk implements Serializable {
    public String chunkHash;
    public int sequenceNumber;
    public int versionNo;
    public String lastModified;
    /* this is the path of the chunk with sequence number */
    public String filePath;
    public int totalChunksCount;
    public int totalShardsCount;

    public List<byte[]> shardsList = new ArrayList<>();
    // public boolean isCorrupted;

    public Chunk(int sequenceNumber, String filePath) {
        this.sequenceNumber = sequenceNumber;
        this.filePath = filePath;
    }

    public void collectShard() {
        /*
         * this will collect all the shards for this chunk
         * make sure to remove it after you are done receiving
         * i.e. sequence number = total count
         */
    }

}
