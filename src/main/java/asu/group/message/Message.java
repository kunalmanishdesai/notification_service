package asu.group.message;

public class Message {

    private final int size;

     private final byte[] message;

    public Message(String message) {
        this.message = message.getBytes();
        this.size = message.length();
    }


    public int getByteArraySize() {
        return size;
    }

    public int getMessageTotalSize() {
        return size+4;
    }

    public byte[] getMessage() {
        return message;
    }
}
