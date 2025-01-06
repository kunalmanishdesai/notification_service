package asu.group.exceptions;

public class MessageIdNotFoundException extends RuntimeException {
    private final long messageId;

    public MessageIdNotFoundException(long messageId) {
        super("Message ID " + messageId + " not found in segment files.");
        this.messageId = messageId;
    }

    public long getMessageId() {
        return messageId;
    }
}
