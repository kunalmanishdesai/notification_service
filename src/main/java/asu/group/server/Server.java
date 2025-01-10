package asu.group.server;

import asu.group.partition.PartitionManager;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Server {

    private final PartitionManager partitionManager;

    public Server(int noOfPartitions) {
        this.partitionManager = new PartitionManager(noOfPartitions,"broker");
    }

    public void startServer() {
        int port = 12345;

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Server is listening on port " + port);

            while (true) {
                Socket socket = serverSocket.accept();
                System.out.println("New client connected: " + socket.getInetAddress());

                new Thread(new ClientHandler(socket, partitionManager)).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Server server = new Server(3); // Example with 3 partitions
        server.startServer();
    }
}
