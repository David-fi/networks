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
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

// DO NOT EDIT starts
interface FullNodeInterface {
    public boolean listen(String ipAddress, int portNumber);
    public void handleIncomingConnections(String startingNodeName, String startingNodeAddress);
}
// DO NOT EDIT ends


public class FullNode implements FullNodeInterface {
    private ServerSocket serverSocket;
    private final Map<String, String> networkMap = new ConcurrentHashMap<>();
    private final Map<String, String> keyValueStore = new ConcurrentHashMap<>();
    public String nodeName;
    public String nodeAddress;

    public boolean listen(String ipAddress, int portNumber) {
        // Implement this!
        // Return true if the node can accept incoming connections
        // Return false otherwise
        try {
            serverSocket = new ServerSocket(portNumber, 50, InetAddress.getByName(ipAddress));
            this.nodeAddress = ipAddress + ":" + portNumber;
            return true;
        } catch (IOException e) {
            System.err.println("Error starting full node: " + e.getMessage());
            return false;
        }
    }
    public void handleIncomingConnections(String startingNodeName, String startingNodeAddress) {
        // Implement this!
        nodeName = startingNodeName;
        //nodeAddress = startingNodeAddress;
        Executors.newSingleThreadExecutor().execute(() -> {
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    notifyOtherFullNodes();
                    handleClient(clientSocket);
                } catch (IOException e) {
                    System.err.println("Failed to accept client: " + e.getMessage());
                }
            }
        });
    }
    private void handleClient(Socket clientSocket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {
            String inputLine = in.readLine();
            if (inputLine != null && inputLine.startsWith("START")) {
                out.println("START 1 " + nodeName);
                System.out.println("START 1 " + nodeName);
            }
            while ((inputLine = in.readLine()) != null) {
                String[] tokens = inputLine.split(" ");
                switch (tokens[0]) {
                    case "START":
                        out.println("STARTED");
                        System.out.println("STARTED");
                        break;
                    case "PUT?":
                        int keyLines = Integer.parseInt(tokens[1]);
                        int valueLines = Integer.parseInt(tokens[2]);
                        StringBuilder keyBuilder = new StringBuilder();
                        StringBuilder valueBuilder = new StringBuilder();
                        for (int i = 0; i < keyLines; i++) {
                            keyBuilder.append(i > 0 ? "\n" : "").append(in.readLine());
                        }
                        for (int i = 0; i < valueLines; i++) {
                            valueBuilder.append(i > 0 ? "\n" : "").append(in.readLine());
                        }
                        String key = keyBuilder.toString();
                        String value = valueBuilder.toString();
                        out.println(handlePutRequest(key, value));
                        System.out.println(handlePutRequest(key, value));
                        break;

                    case "GET?":
                        keyLines = Integer.parseInt(tokens[1]);
                        keyBuilder = new StringBuilder();
                        for (int i = 0; i < keyLines; i++) {
                            keyBuilder.append(i > 0 ? "\n" : "").append(in.readLine());
                        }
                        key = keyBuilder.toString();
                        String retrievedValue = handleGetRequest(key);
                        if (retrievedValue != null) {
                            String[] valueLines2 = retrievedValue.split("\n");

                            out.println("VALUE " + valueLines2.length);
                            out.println(retrievedValue);
                            System.out.println("VALUE " + valueLines2.length);
                            System.out.println(retrievedValue);
                        } else {
                            out.println("NOPE");
                        }
                        break;
                    case "NOTIFY":
                        String nodeName = in.readLine();
                        String nodeAddress = in.readLine();
                        networkMap.put(nodeName, nodeAddress);
                        out.println("NOTIFIED");
                        System.out.println("NOTIFIED");
                        System.out.println("Received NOTIFY from " + nodeName + " with address " + nodeAddress);
                        break;
                    case "NEAREST?":
                        String hashID = tokens[1];
                        List<Map.Entry<String, String>> closestNodes = findClosestNodes(hashID);
                        if (closestNodes != null && !closestNodes.isEmpty()) {
                            out.println("NODES " + closestNodes.size());
                            for (Map.Entry<String, String> node : closestNodes) {
                                out.println(node.getKey() + "," + node.getValue());
                            }
                        } else {
                            out.println("NODES 0");
                        }
                        break;
                    case "ECHO?":
                        out.println("OHCE");
                        System.out.println("OHCE");
                        break;
                    case "END":
                        String reason = tokens.length > 1 ? tokens[1] : "No reason provided";
                        System.out.println("Connection ended by client: " + reason);
                        clientSocket.close();
                        return;
                }
            }
        } catch (IOException e) {
            System.err.println("Error handling client: " + e.getMessage());
        }
    }

    private String handlePutRequest(String key, String value) {
        String keyHashID = computeHashID(key);
         List<Map.Entry<String, String>> closestNodes = findClosestNodes(keyHashID);

         boolean shouldStore = isOneOfTheClosestNodes(closestNodes, this.nodeAddress);
        // if (shouldStore) {
        keyValueStore.put(key, value);
        return "SUCCESS";}
    // else {
        // return "FAILED";
    // }
     // }
    private String handleGetRequest(String key) {
        String value = keyValueStore.get(key);
        if (value != null) {
            return "VALUE "+  value.split("\n", -1).length + "\n" + value;
        } else {
            return "NOPE";
        }
    }
    private String computeHashID(String text) {
        try {
            // Ensure the newline character is included at the end of the text.
            String textWithNewline = text.endsWith("\n") ? text : text + "\n";
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            // Hash the text (with newline character).
            byte[] hash = digest.digest(textWithNewline.getBytes(StandardCharsets.UTF_8));
            // Convert the byte array to a hex string.
            StringBuilder hexString = new StringBuilder();
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Could not find hash algorithm", e);
        }
    }
    private int computeHashDistance(String hashID1, String hashID2) {
        // Convert hex strings to BigInteger for bitwise operations.
        BigInteger id1 = new BigInteger(hashID1, 16);
        BigInteger id2 = new BigInteger(hashID2, 16);
        // XOR the two hashIDs to find differing bits.
        BigInteger xorResult = id1.xor(id2);
        // The distance is 256 minus the number of leading zeros in the XOR result.
        // Note: BigInteger.bitLength() returns the length of the minimal two's-complement representation, excluding the sign bit.
        int distance = 256 - xorResult.bitLength(); // Adjusted to compute distance as specified.
        return distance;
    }
    private List<Map.Entry<String, String>> findClosestNodes(String hashID) {
        List<Map.Entry<String, String>> sortedNodes = new ArrayList<>(networkMap.entrySet());

        // Use computeHashDistance to sort nodes by their distance to hashID.
        sortedNodes.sort((node1, node2) -> {
            String hashID1 = computeHashID(node1.getKey());
            String hashID2 = computeHashID(node2.getKey());
            int distance1 = computeHashDistance(hashID, hashID1);
            int distance2 = computeHashDistance(hashID, hashID2);
            return Integer.compare(distance1, distance2);
        });

        // Return the top three closest nodes, or fewer if less than three are available.
        return sortedNodes.subList(0, Math.min(3, sortedNodes.size()));
    }
    private boolean isOneOfTheClosestNodes(List<Map.Entry<String, String>> nodes, String nodeAddress) {
        for (Map.Entry<String, String> entry : nodes) {
            if (entry.getValue().equals(nodeAddress)) {
                return true;
            }
        }
        return false;
    }
    private void notifyOtherFullNodes() {
        for (String otherNodeName : networkMap.keySet()) {
            String otherNodeAddress = networkMap.get(otherNodeName);
            try {
                Socket socket = new Socket(otherNodeAddress.split(":")[0], Integer.parseInt(otherNodeAddress.split(":")[1]));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                // Send NOTIFY followed by this node's name and address
                out.println("NOTIFY");
                out.println(nodeName);
                out.println(nodeAddress);
                socket.close();
            } catch (IOException e) {
                System.err.println("Could not notify " + otherNodeName + " at " + otherNodeAddress + ": " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        FullNode node = new FullNode();
        if (node.listen("localhost", 1400)) {
            System.out.println("Full Node listening on localhost:1400");
            node.handleIncomingConnections("david.ferreira-inacio@city.ac.uk:YourNodeName", "127.0.0.1:1400");
            node.notifyOtherFullNodes();
        }
    }


}
