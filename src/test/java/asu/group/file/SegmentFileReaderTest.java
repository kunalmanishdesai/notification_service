package asu.group.file;

import asu.group.exceptions.MessageIdNotFoundException;
import asu.group.message.Message;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.*;

class SegmentFileReaderTest {

    private static final String TEST_FILENAME = "test_segment_reader.log";

    private final int baseOffset = 0;
    private SegmentFile segmentFile;
    private SegmentFileReader segmentFileReader;

    @BeforeEach
    void setUp() throws IOException {
        SegmentFileMetadata metadata = new SegmentFileMetadata(TEST_FILENAME, baseOffset);
        segmentFile = new SegmentFile(metadata, true);

        // Add some messages to the file for testing
        segmentFile.append(new Message("Message 1"));
        segmentFile.append(new Message("Message 2"));
        segmentFile.append(new Message("Message 3"));
        segmentFile.close();

        segmentFileReader = new SegmentFileReader(TEST_FILENAME + baseOffset);
    }

    @AfterEach
    void tearDown() throws IOException {
        Files.deleteIfExists(Path.of(TEST_FILENAME + baseOffset));
    }

    @Test
    void testReadValidMessage() {
        String message = segmentFileReader.read(baseOffset); // Reading the first message
        assertEquals("Message 1", message, "Should correctly read the first message.");

        message = segmentFileReader.read(4 + "Message 1".length()); // Reading the second message
        assertEquals("Message 2", message, "Should correctly read the second message.");
    }

    @Test
    void testReadInvalidMessageId() {
        assertThrows(MessageIdNotFoundException.class, () -> segmentFileReader.read(-1), "Should throw exception for negative ID.");
        assertThrows(MessageIdNotFoundException.class, () -> segmentFileReader.read(10000), "Should throw exception for out-of-bounds ID.");
    }

}
