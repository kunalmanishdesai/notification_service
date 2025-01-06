package asu.group.file;

import asu.group.message.Message;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class SegmentFileTest {

    private static final String TEST_FILENAME = "test_segment.log";

    private final int baseOffset = 0;
    private SegmentFile segmentFile;

    @BeforeEach
    void setUp() throws IOException {
        SegmentFileMetadata metadata = new SegmentFileMetadata(TEST_FILENAME,baseOffset);
        segmentFile = new SegmentFile(metadata, true);
    }

    @AfterEach
    void tearDown() throws IOException {
        segmentFile.close();
        Files.delete(Path.of(TEST_FILENAME + baseOffset));
    }

    @Test
    void testAppendMessage() throws IOException {

        String messageString = "Hello, Kafka Clone!";
        Message message = new Message(messageString);
        segmentFile.append(message);
        assertEquals(message.getMessageTotalSize(), messageString.getBytes().length + 4, "Offset should be updated correctly after appending.");
    }

    @Test
    void testShouldRoll() {
        // Create a large string that exceeds the segment file size
        String largeMessageContent = "A".repeat(SegmentFile.FILE_SIZE + 1);
        Message largeMessage = new Message(largeMessageContent);
        assertTrue(segmentFile.shouldRoll(largeMessage), "Should roll when message size exceeds file size.");

        // Create a small string that is within the segment file size
        String smallMessageContent = "A";
        Message smallMessage = new Message(smallMessageContent);
        assertFalse(segmentFile.shouldRoll(smallMessage), "Should not roll when message size is within file size.");
    }

    @Test
    void testClose() {
        assertDoesNotThrow(() -> segmentFile.close(), "SegmentFile should close without exceptions.");
    }

    @Test
    void testGetName() {
        assertEquals(TEST_FILENAME + baseOffset, segmentFile.getName(), "File name should match the provided name.");
    }
}
