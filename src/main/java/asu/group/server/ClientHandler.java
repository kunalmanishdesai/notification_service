package asu.group.server;

import asu.group.partition.PartitionManager;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.ExecutionException;

public class ClientHandler implements Runnable {
    private final Socket clientSocket;
    private final PartitionManager partitionManager;

    public ClientHandler(Socket socket, PartitionManager partitionManager) {
        this.clientSocket = socket;
        this.partitionManager = partitionManager;
    }

    @Override
    public void run() {
        try (
                BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true)
        ) {
            writer.println("Welcome to the Persistent Server!");

            String input;
            while ((input = reader.readLine()) != null) {
                System.out.println("Received from client: " + input);

                Request request = Request.parse(input);
                Response response = handleRequest(request);
                writer.println(response);

                if ("exit".equalsIgnoreCase(request.getCommand())) {
                    System.out.println("Client requested to disconnect.");
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                clientSocket.close();
                System.out.println("Client disconnected.");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private Response handleRequest(Request request) {
        try {
            switch (request.getCommand().toLowerCase()) {
                case "append":
                    if (request.getArguments().length < 1) {
                        return new Response(false, "Append command requires data.");
                    }
                    String data = String.join(" ", request.getArguments());
                    partitionManager.append(data).get(); // Wait for the append operation to complete
                    return new Response(true, "Data appended successfully: " + data);

                case "read":
                    if (request.getArguments().length != 2) {
                        return new Response(false, "Read command requires a partition ID and message ID.");
                    }
                    String partitionId = request.getArguments()[0];
                    int messageId = Integer.parseInt(request.getArguments()[1]);
                    String result = partitionManager.read(partitionId, messageId).get(); // Blocking call
                    return new Response(true, "Read result: " + result);

                case "exit":
                    return new Response(true, "Goodbye!");

                default:
                    return new Response(false, "Unknown command. Supported commands: append <data>, read <partitionId> <messageId>, exit.");
            }
        } catch (NumberFormatException e) {
            return new Response(false, "Invalid message ID format.");
        } catch (ExecutionException | InterruptedException e) {
            return new Response(false, "Error processing request: " + e.getMessage());
        }
    }
}
