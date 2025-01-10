package asu.group.partition;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PartitionScheduler {
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public static void start(PartitionManager partitionManager, int saveIntervalHours, int deleteIntervalDays) {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                partitionManager.saveFiles();
            } catch (IOException e) {
                System.err.println("Error during saveFiles: " + e.getMessage());
            }
        }, 0, saveIntervalHours, TimeUnit.HOURS);

        scheduler.scheduleAtFixedRate(() -> {
            try {
                partitionManager.deleteFiles();
            } catch (IOException e) {
                System.err.println("Error during deleteFiles: " + e.getMessage());
            }
        }, 0, deleteIntervalDays, TimeUnit.DAYS);
    }

    public static void stop() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
        }
    }

}
