package asu.group.partition;

import org.apache.commons.lang3.RandomStringUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class PartitionManager {

    private final int noOfPartitions;
    private final ArrayList<Partition> partitions;
    private final String brokerName;
    private final ExecutorService executor;
    private int currentPartitionIndex = 0;

    public PartitionManager(int noOfPartitions, String brokerName) {
        this.noOfPartitions = noOfPartitions;
        this.brokerName = brokerName;
        this.partitions = loadOrCreatePartitions();
        this.executor = Executors.newCachedThreadPool();
        PartitionScheduler.start(this, 1, 7);
    }

    private ArrayList<Partition> loadOrCreatePartitions() {
        File file = new File(brokerName);
        try {
            if (file.exists()) {
                return PartitionFileUtil.loadPartitionsFromFile(file);
            } else {
                ArrayList<Partition> newPartitions = createNewPartitions(noOfPartitions);
                PartitionFileUtil.savePartitionNamesToFile(newPartitions, file);
                return newPartitions;
            }
        } catch (IOException e) {
            throw new RuntimeException("Error loading or creating partitions", e);
        }
    }

    private ArrayList<Partition> createNewPartitions(int size) {
        ArrayList<Partition> partitions = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            try {
                partitions.add(new Partition(RandomStringUtils.randomAlphanumeric(17).toUpperCase()));
            } catch (IOException e) {
                throw new RuntimeException("Error creating partition", e);
            }
        }
        return partitions;
    }

    public Future<Void> append(String data) {
        return executor.submit(() -> {
            try {
                Partition currentPartition;

                synchronized (this) {
                    currentPartition = partitions.get(currentPartitionIndex);
                    currentPartitionIndex = (currentPartitionIndex + 1) % noOfPartitions;
                }

                // Append data to the current partition (Partition handles its own locking)
                currentPartition.append(data);
                return null;
            } catch (IOException e) {
                throw new RuntimeException("Error appending data to partition", e);
            }
        });
    }

    public Future<String> read(String partitionPrefix, int messageId) {
        return executor.submit(() -> getPartitionById(partitionPrefix).read(messageId));
    }

    private Partition getPartitionById(String prefix) {
        return partitions.stream()
                .filter(partition -> partition.getPrefix().equals(prefix))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Partition not found: " + prefix));
    }

    public ArrayList<Partition> getPartitions() {
        return partitions;
    }

    public void saveFiles() throws IOException {
        PartitionFileUtil.saveAllPartitions(this);
    }

    public void deleteFiles() throws IOException {
        PartitionFileUtil.deleteOldFiles(this, 7);
    }

    public void shutdown() {
        PartitionScheduler.stop();
        executor.shutdown();
    }
}
