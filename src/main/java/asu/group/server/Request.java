package asu.group.server;

public class Request {
    private final String command;
    private final String[] arguments;

    public Request(String command, String[] arguments) {
        this.command = command;
        this.arguments = arguments;
    }

    public String getCommand() {
        return command;
    }

    public String[] getArguments() {
        return arguments;
    }

    public static Request parse(String input) {
        String[] parts = input.split(" ", 2);
        String command = parts[0];
        String[] arguments = parts.length > 1 ? parts[1].split(" ") : new String[0];
        return new Request(command, arguments);
    }
}
