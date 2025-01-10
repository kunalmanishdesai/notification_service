package asu.group.partition;

import asu.group.exceptions.MessageIdNotFoundException;
import asu.group.file.SegmentFile;
import asu.group.file.SegmentFileMetadata;
import asu.group.file.SegmentFileReader;
import asu.group.message.Message;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Partition {

    public Partition(String prefix) throws IOException {
        this.prefix = prefix;

        Path path = Paths.get(prefix);

         if (!Files.exists(path)) {
             Files.createFile(path);
             segmentFileMetadataList = new ArrayList<>();
         } else {
             segmentFileMetadataList = readFromFile(prefix);
         }
        partitionFileWriter = new ObjectOutputStream(new FileOutputStream(prefix,true));
    }

    private List<SegmentFileMetadata> readFromFile(String filename) {
        List<SegmentFileMetadata> segmentFileMetadataList = new ArrayList<>();

        try (FileInputStream fileInputStream = new FileInputStream(filename);
             ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {

            while (true) {
                try {
                    // Read an object from the stream
                    SegmentFileMetadata segmentFileMetadata = (SegmentFileMetadata) objectInputStream.readObject();
                    segmentFileMetadataList.add(segmentFileMetadata);
                } catch (EOFException e) {
                    // End of file reached
                    break;
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Error reading from file: " + e.getMessage());
            e.printStackTrace();
        }
        return segmentFileMetadataList;
    }

    private final ObjectOutputStream partitionFileWriter;

    public String getPrefix() {
        return prefix;
    }

    private final String prefix;

    private int baseOffset = 0;

    private SegmentFile activeFile;
    private final Lock lock = new ReentrantLock();

    private final List<SegmentFileMetadata> segmentFileMetadataList;

    public void append(String message) throws IOException {

        lock.lock();
        Message message1 = new Message(message);

        // roll if no segment file is present or activeFile is full
        if (segmentFileMetadataList.isEmpty() || activeFile == null || activeFile.shouldRoll(message1)) {

            if (activeFile != null) {
                activeFile.close();
            }

            SegmentFileMetadata activeFileMetadata = new SegmentFileMetadata(prefix, baseOffset);

            partitionFileWriter.writeObject(activeFileMetadata);
            partitionFileWriter.flush();

            activeFile = new SegmentFile(activeFileMetadata, true);
            segmentFileMetadataList.add(activeFileMetadata);
        }

        activeFile.append(message1);
        baseOffset += message1.getMessageTotalSize();
        lock.unlock();
    }

    public SegmentFileMetadata getSegmentFile(int messageId) {
        ListIterator<SegmentFileMetadata> iterator = segmentFileMetadataList.listIterator(segmentFileMetadataList.size());

        while (iterator.hasPrevious()) {
            SegmentFileMetadata segmentFileMetadata = iterator.previous();
            if (messageId >= segmentFileMetadata.getBaseOffset()) {
                return segmentFileMetadata;
            }
        }

        throw new MessageIdNotFoundException(messageId);
    }

    public String read(int messageId) throws Exception {
        SegmentFileMetadata segmentFileMetadata = getSegmentFile(messageId);
        return new SegmentFileReader(segmentFileMetadata.getFilename())
                .read(messageId-segmentFileMetadata.getBaseOffset());
    }

    public List<SegmentFileMetadata> getSegmentFileMetadataList() {
        return segmentFileMetadataList;
    }

    public void close() throws IOException {
        if (activeFile != null) {
            activeFile.close();
        }
    }

}
