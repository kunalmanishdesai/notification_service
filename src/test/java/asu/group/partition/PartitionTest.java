package asu.group.partition;

import asu.group.exceptions.MessageIdNotFoundException;
import asu.group.file.SegmentFile;
import asu.group.file.SegmentFileMetadata;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class PartitionTest {

    private static final String TEST_PARTITION_PREFIX = "test_partition";
    private Partition partition;

    @BeforeEach
    void setUp() throws IOException {
        partition = new Partition(TEST_PARTITION_PREFIX);
    }

    @AfterEach
    void tearDown() throws IOException {
        Files.deleteIfExists(Path.of(TEST_PARTITION_PREFIX));
        // Clean up any segment files created during the tests
        Files.list(Path.of("."))
                .filter(path -> path.toString().startsWith("./" + TEST_PARTITION_PREFIX))
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException ignored) {}
                });

        Files.deleteIfExists(Path.of(TEST_PARTITION_PREFIX+0));
    }

    @Test
    void testAppendAndRead() throws Exception {
        String message1 = "Message 1";
        String message2 = "Message 2";

        partition.append(message1);
        partition.append(message2);

        assertEquals(message1, partition.read(0), "Should correctly read the first message.");
        assertEquals(message2, partition.read(message1.length() + 4), "Should correctly read the second message.");
    }

    @Test
    void testRollingSegments() throws IOException {
        // Add messages until a roll is required
        String largeMessage = "A".repeat(SegmentFile.FILE_SIZE-4);
        partition.append(largeMessage);

        String newMessage = "New Message";
        partition.append(newMessage);

        SegmentFileMetadata metadata = partition.getSegmentFile(SegmentFile.FILE_SIZE);
        assertNotNull(metadata, "Should create a new segment after rolling.");
        assertEquals(SegmentFile.FILE_SIZE, metadata.getBaseOffset(), "Base offset of the new segment should match the roll condition.");
    }

    @Test
    void testGetSegmentFileForInvalidId() {
        assertThrows(MessageIdNotFoundException.class, () -> partition.getSegmentFile(-1), "Should throw exception for negative message ID.");
    }

    @Test
    void testConcurrentAppend() throws InterruptedException {
        Thread thread1 = new Thread(() -> {
            try {
                partition.append("Thread 1 - Message 1");
                partition.append("Thread 1 - Message 2");
            } catch (IOException e) {
                fail("Thread 1 failed to append messages.");
            }
        });

        Thread thread2 = new Thread(() -> {
            try {
                partition.append("Thread 2 - Message 1");
                partition.append("Thread 2 - Message 2");
            } catch (IOException e) {
                fail("Thread 2 failed to append messages.");
            }
        });

        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();

        // Validate that messages from both threads exist
        assertDoesNotThrow(() -> partition.read(0), "Messages should be correctly written concurrently.");
    }

    @Test
    void testClosePartition() throws IOException {
        partition.append("Message before closing.");
        assertDoesNotThrow(() -> partition.close(), "Partition should close without issues.");
    }
}
