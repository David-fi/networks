// IN2011 Computer Networks
// Coursework 2023/2024
//
// Submission by
// David Ferreira Inacio
// 220057994
// david.ferreira-inacio@city.ac.uk


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;


// DO NOT EDIT starts
interface TemporaryNodeInterface {
    public boolean start(String startingNodeName, String startingNodeAddress);
    public boolean store(String key, String value);
    public String get(String key);
}
// DO NOT EDIT ends



public class TemporaryNode implements TemporaryNodeInterface {

    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;

    @Override
    public boolean start(String startingNodeName, String startingNodeAddress) {
        // Implement this!
        // Return true if the 2D#4 network can be contacted
        // Return false if the 2D#4 network can't be contacted
        try {
            String[] addressComponents = startingNodeAddress.split(":");
            this.socket = new Socket(addressComponents[0], Integer.parseInt(addressComponents[1]));
            this.out = new PrintWriter(socket.getOutputStream(), true);
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            sendMessage("START 1 " + startingNodeName);
            String response = in.readLine();
            if (response.startsWith("START")) {
                return true;
            } else {
                System.err.println("Failed to start: Node did not acknowledge START command.");
                return false;
            }
        } catch (IOException e) {
            System.err.println("Error starting the node: " + e.getMessage());
            return false;
        }
    }

    @Override
    public boolean store(String key, String value) {
        // Implement this!
        // Return true if the store worked
        // Return false if the store failed
        try {
            int keyLines = key.split("\n").length;
            int valueLines = value.split("\n").length;

            sendMessage("PUT? " + keyLines + " " + valueLines);

            sendMessage(key);
            sendMessage(value);

            String response = in.readLine();

            return "SUCCESS".equals(response);
        } catch (IOException e) {
            System.err.println("Error storing the value: " + e.getMessage());
            return false;
        }
    }

    @Override
    public String get(String key) {
        // Implement this!
        // Return the string if the get worked
        // Return null if it didn't
        try {
            int keyLines = key.split("\n").length;

            sendMessage("GET? " + keyLines);
            sendMessage(key);

            String response = in.readLine();

            if (response != null && response.startsWith("VALUE")) {
                int valueLines = Integer.parseInt(response.split(" ")[1]);
                StringBuilder valueBuilder = new StringBuilder();
                for (int i = 0; i < valueLines; i++) {
                    valueBuilder.append(i > 0 ? "\n" : "").append(in.readLine());
                }
                return valueBuilder.toString();
            } else {
                return null;
            }
        } catch (IOException e) {
            System.err.println("Error retrieving the value: " + e.getMessage());
            return null;
        }
    }

    private void sendMessage(String message) {
        String[] lines = message.split("\n");
        for (String line : lines) {
            out.println(line);
            System.out.println(line);
            out.flush();
        }
    }

    public void endConnection(String reason) {
        sendMessage("END " + reason);
        try {
            socket.close();
        } catch (IOException e) {
            System.err.println("Error while closing the connection: " + e.getMessage());
        }
    }

    public boolean sendEchoRequest() {
        try {
            sendMessage("ECHO?");
            String response = in.readLine();
            if ("OHCE".equals(response)) {
                System.out.println("Echo response received successfully.");
                return true;
            } else {
                System.err.println("Failed to receive correct echo response.");
                return false;
            }
        } catch (IOException e) {
            System.err.println("Error sending ECHO request: " + e.getMessage());
            return false;
        }
    }

    public void findNearestNodes(String key) {
        try {
            sendMessage("NEAREST? " + key);

            String response = in.readLine();
            String[] responseParts = response.split(" ");
            if ("NODES".equals(responseParts[0]) && responseParts.length > 1) {
                int numberOfNodes = Integer.parseInt(responseParts[1]);
                System.out.println("Closest nodes:");
                for (int i = 0; i < numberOfNodes; i++) {
                    System.out.println(in.readLine());
                }
            } else {
                System.out.println("Failed to retrieve closest nodes or no closest nodes available.");
            }
        } catch (IOException e) {
            System.err.println("Error finding the nearest nodes: " + e.getMessage());
        }
    }

    public void notifyNode(String nodeName, String nodeAddress, String targetNodeName, String targetNodeAddress) {
        try {
            Socket socket = new Socket(targetNodeAddress.split(":")[0], Integer.parseInt(targetNodeAddress.split(":")[1]));
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            sendMessage("NOTIFY");
            sendMessage(nodeName);
            sendMessage(nodeAddress);

            String response = in.readLine();
            if ("NOTIFIED".equals(response)) {
                System.out.println("FullNode " + targetNodeName + " has been successfully notified about this node.");
            } else {
                System.err.println("Failed to notify FullNode " + targetNodeName + ": " + response);
            }

            socket.close();
        } catch (IOException e) {
            System.err.println("Error notifying FullNode " + targetNodeName + ": " + e.getMessage());
        }
    }



 }






