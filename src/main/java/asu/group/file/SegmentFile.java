package asu.group.file;

import asu.group.message.Message;

import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * A segment of the log. Each segment has two components: a log and an index.
 * The log is a FileRecords containing the actual messages. The index is an
 * OffsetIndex (not included here) that maps from logical offsets to physical
 * file positions. Each segment has a base offset which is an offset <= the
 * least offset of any message in this segment and > any offset in any
 * previous segment.
 *
 * <p>
 * A segment with a base offset of [base_offset] would be stored in two files, a
 * [base_offset].index and a [base_offset].log file.
 *
 * <p>
 * This class is not thread-safe.
 */
public class SegmentFile implements Closeable {

    private final String name;

    private final Path filePath;
    private final FileOutputStream outputStream; // Use FileOutputStream for proper writing
    private int offset;

    public static final int FILE_SIZE = 1024 * 1024 * 1024; // Use final for constants

    public SegmentFile(SegmentFileMetadata segmentFileMetadata, boolean createFile) throws IOException {
        name = segmentFileMetadata.getFilename();
        filePath = Paths.get(name);


        if (createFile) {
            Files.createFile(filePath);
        }

        outputStream = new FileOutputStream(name, true); // Open in append mode
        offset = 0;
    }


    public void append(Message message) throws IOException {

        if (offset + message.getMessageTotalSize() > FILE_SIZE) {
            throw new IOException("Segment full"); // More descriptive exception
        }
        // write size
        outputStream.write(ByteBuffer.allocate(4).putInt(message.getByteArraySize()).array());

        // write byte array
        outputStream.write(message.getMessage());
        offset += message.getMessageTotalSize();
    }

    public void save() throws IOException {
        outputStream.flush();
    }


    public boolean shouldRoll(Message message) {
        return offset + message.getMessageTotalSize() > FILE_SIZE;
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
    }

    public String getName() {
        return name;
    }
}