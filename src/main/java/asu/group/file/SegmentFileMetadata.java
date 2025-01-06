package asu.group.file;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

public class SegmentFileMetadata implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Instant creationTime;
    private final String prefix;
    private final int baseOffset;

    public SegmentFileMetadata(String filename, int baseOffset) {
        this(filename, baseOffset, Instant.now()); // Default to current time
    }

    public SegmentFileMetadata(String prefix, int baseOffset, Instant creationTime) {
        this.prefix = prefix;
        this.baseOffset = baseOffset;
        this.creationTime = creationTime;
    }

    public Instant getCreationTime() {
        return creationTime;
    }

    public String getFilename() {
        return prefix + baseOffset;
    }

    public int getBaseOffset() {
        return baseOffset;
    }

    @Override
    public String toString() {
        return "SegmentFileMetadata{" +
                "creationTime=" + creationTime +
                ", filename='" + prefix + '\'' +
                ", baseOffset=" + baseOffset +
                '}';
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (!(object instanceof SegmentFileMetadata that)) return false;
        return baseOffset == that.baseOffset && Objects.equals(creationTime, that.creationTime) && Objects.equals(prefix, that.prefix);
    }

    @Override
    public int hashCode() {
        return Objects.hash(creationTime, prefix, baseOffset);
    }
}