package asu.group.file;

import asu.group.exceptions.MessageIdNotFoundException;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class SegmentFileReader {

    private final MappedByteBuffer mappedBuffer;

    public SegmentFileReader(String filename) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(Path.of(filename), StandardOpenOption.READ)) {
            this.mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
        }
    }

    /**
     * Reads a message at the given messageId, which is derived from baseOffset + position.
     *
     * @param messageId The offset position in the file.
     * @return The message as a string if found; otherwise, null.
     * @throws IOException If there is an error reading the file.
     */
    public String read(int messageId) {
        if (messageId < 0 || messageId >= mappedBuffer.capacity()) {
            throw new MessageIdNotFoundException(messageId);
        }

        try {
            // Move the buffer's position to the messageId offset
            mappedBuffer.position((int) messageId);

            // Read the length of the message (assuming it's stored as an int)
            int messageLength = mappedBuffer.getInt();

            // Read the message content as a byte array
            byte[] messageBytes = new byte[messageLength];
            mappedBuffer.get(messageBytes);

            // Convert the byte array to a string and return
            return new String(messageBytes);
        } catch (Exception e) {
            // Handle unexpected EOF or other errors gracefully
            System.err.println("Error reading message at ID " + messageId + ": " + e.getMessage());
            return null;
        }
    }
}
