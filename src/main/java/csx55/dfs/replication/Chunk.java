package csx55.dfs.replication;

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
    public List<String> sliceHashes = new ArrayList<>();
    // public boolean isCorrupted;

    public Chunk(int sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
        this.lastModified = getCurrentTime();
        this.versionNo = 0;
    }

    private String getCurrentTime() {
        LocalDateTime currentTime = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String currentTimeString = currentTime.format(formatter);
        return currentTimeString;
    }

    public boolean writeChunk(String chunkPathPrefix, String chunkPath, int sequence, byte[] chunk) {
        /*
         * first create the filename of the chunk with sequence number like this
         * /SimFile.data_chunk2
         * write it to the chunk directory
         * keep track of it in the chunks list and new chunks list for the heartbeat
         * then, create 8KB slices of the chunk and generate sha-1 checksum for each
         * add those hashes into the list of the chunk object
         */
        String uploadPath = chunkPathPrefix + chunkPath + "_chunk" + sequence;
        this.filePath = uploadPath;
        this.chunkHash = getDigest(chunk);
        File chunkFile = new File(uploadPath);

        try {
            chunkFile.getParentFile().mkdirs();
            Path filePath = Paths.get(uploadPath);
            if (!Files.exists(filePath)) {
                // Create a new file
                Files.createFile(filePath);
            }
            Files.write(filePath, chunk);
            System.out.println("Successfully wrote chunk: " + uploadPath);

            return true;
        } catch (IOException e) {
            System.out.println("Error occurred while trying to write chunk: " + uploadPath +
                    e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    public void createChecksumSlices(byte[] chunk) {
        List<byte[]> slices = new ArrayList<>();
        int offset = 0;
        int sliceSize = 8 * 1024; // 8KB
        int length;

        try {
            while (offset < chunk.length) {
                /* keeping length of bytes to read either chunksize or remaining bytes left */
                length = Math.min(sliceSize, chunk.length - offset);
                byte[] slice = new byte[length];
                System.arraycopy(chunk, offset, slice, 0, length);
                slices.add(slice);

                /* also create the checksum for this slice and add it to hashes list */
                String sliceDigest = getDigest(slice);
                sliceHashes.add(sliceDigest);

                offset += length;
            }
        } catch (Exception e) {
            System.out.println("Error while generating slices and their checksum: " + e.getMessage());
            e.printStackTrace();
        }

    }

    public List<Integer> findCorruptedSlice(byte[] corruptedChunk) {
        int offset = 0;
        int sliceSize = 8 * 1024; // 8KB
        int length;

        // int originalSize = sliceHashes.size();
        List<Integer> corruptedSliceIndexes = new ArrayList<>();
        int sliceIndex = 0;

        /*
         * TODO: corrupted slice isnt correct
         * fix:low priority
         */
        while (offset < corruptedChunk.length) {
            /* keeping length of bytes to read either chunksize or remaining bytes left */
            length = Math.min(sliceSize, corruptedChunk.length - offset);
            byte[] slice = new byte[length];
            System.arraycopy(corruptedChunk, offset, slice, 0, length);

            /* also create the checksum for this slice and add it to hashes list */

            String sliceDigest = getDigest(slice);
       

            if (!sliceDigest.equals(sliceHashes.get(sliceIndex))) {
                corruptedSliceIndexes.add(sliceIndex);
            }

            offset += length;
            sliceIndex += 1;
        }
        return corruptedSliceIndexes;

    }

    public String getDigest(byte[] slice) {
        try {
            MessageDigest instance = MessageDigest.getInstance("SHA-1");
            instance.update(slice);
            byte[] hashBytes = instance.digest();
            StringBuilder hexString = new StringBuilder();
            for (byte b : hashBytes) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            return hexString.toString();

        } catch (Exception e) {
            System.out.println("Error generating message digest for slice: " + e.getMessage());
            e.printStackTrace();
            return "";
        }
    }

}
