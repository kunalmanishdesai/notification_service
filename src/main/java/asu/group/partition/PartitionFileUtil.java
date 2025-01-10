package asu.group.partition;

import asu.group.file.SegmentFile;
import asu.group.file.SegmentFileMetadata;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;

public class PartitionFileUtil {

    public static ArrayList<Partition> loadPartitionsFromFile(File file) throws IOException {
        ArrayList<Partition> partitions = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                partitions.add(new Partition(line.trim()));
            }
        }
        return partitions;
    }

    public static void savePartitionNamesToFile(ArrayList<Partition> partitions, File file) throws IOException {
        try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
            for (Partition partition : partitions) {
                writer.println(partition.getPrefix());
            }
        }
    }

    public static void saveAllPartitions(PartitionManager partitionManager) throws IOException {
        for (Partition partition : partitionManager.getPartitions()) {
            for (SegmentFileMetadata segmentFileMetadata : partition.getSegmentFileMetadataList()) {
                SegmentFile segmentFile = new SegmentFile(segmentFileMetadata, false);
                segmentFile.save();
            }
        }
    }

    public static void deleteOldFiles(PartitionManager partitionManager, int retentionDays) throws IOException {
        for (Partition partition : partitionManager.getPartitions()) {
            for (SegmentFileMetadata segmentFileMetadata : partition.getSegmentFileMetadataList()) {
                if (Duration.between(segmentFileMetadata.getCreationTime(), Instant.now()).toDays() > retentionDays) {
                    Path path = Path.of(segmentFileMetadata.getFilename());
                    Files.deleteIfExists(path);
                }
            }
        }
    }
}
