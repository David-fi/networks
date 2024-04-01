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
    private final Map<Node, Integer> networkMap = new ConcurrentHashMap<>();
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
        networkMap.put(new Node(startingNodeName, startingNodeAddress), 0);
        //nodeAddress = startingNodeAddress;
        Executors.newSingleThreadExecutor().execute(() -> {
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    Executors.newSingleThreadExecutor().execute(() -> handleClient(clientSocket));
                    notifyOtherFullNodes();
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
                        networkMap.put(new Node(nodeName, nodeAddress), findDistanceToKey(nodeName));
                        out.println("NOTIFIED");
                        System.out.println("NOTIFIED");
                        System.out.println("Received NOTIFY from " + nodeName + " with address " + nodeAddress);
                        break;
                    case "NEAREST?":
                        String clientKey = tokens[1];

                        List<Node> closestNodes = findClosestNodes(findDistanceToKey(clientKey));
                        if (!closestNodes.isEmpty()) {
                            out.println("NODES " + closestNodes.size());
                            for (Node node : closestNodes) {
                                out.println(node.nodeName + "," + node.nodeAddress);
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
        try {
            byte[] keyHashID = HashID.computeHashID(key + "\n");
            byte[] currentNodeHasId = HashID.computeHashID(nodeName + "\n");

            int distance = calculateDistanceBetweenNodes(keyHashID, currentNodeHasId);

            List<Node> closestNodes = findClosestNodes(distance);

            boolean shouldStore = false;
            for (Node closestNode : closestNodes) {
                if (closestNode.nodeName.equals(nodeName)){
                    shouldStore = true;
                    break;
                }
            }

            if (shouldStore) {
                keyValueStore.put(key, value);
                return "SUCCESS";
            } else {
                return "FAILED";
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String handleGetRequest(String key) {
        String value = keyValueStore.get(key);
        if (value != null) {
            return "VALUE "+  value.split("\n", -1).length + "\n" + value;
        } else {
            return "NOPE";
        }
    }

    private Integer findDistanceToKey(String key) {
        try {
            byte[] hashedNodeName = HashID.computeHashID(nodeName + "\n");
            byte[] hashedKey = HashID.computeHashID(key + "\n");
            return calculateDistanceBetweenNodes(hashedNodeName, hashedKey);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static int calculateDistanceBetweenNodes(byte[] hashedNodeOne, byte[] hashedNodeTwo) {
        int matchingLeadingBits = countMatchingLeadingBits(hashedNodeOne, hashedNodeTwo);
        return 256 - matchingLeadingBits;
    }

    private static int countMatchingLeadingBits(byte[] array1, byte[] array2) {
        int matchingBits = 0;
        int minLength = Math.min(array1.length, array2.length);

        for (int i = 0; i < minLength; i++) {
            byte b1 = array1[i];
            byte b2 = array2[i];

            if (b1 == b2) {
                matchingBits += 8;
            } else {
                int matchingBitsInByte = countMatchingLeadingBitsInByte(b1, b2);
                matchingBits += matchingBitsInByte;
                break;
            }
        }

        return matchingBits;
    }

    private static int countMatchingLeadingBitsInByte(byte b1, byte b2) {
        int count = 0;
        int xor = b1 ^ b2;

        for (int i = 7; i >= 0; i--) {
            if ((xor & (1 << i)) == 0) {
                count++;
            } else {
                break;
            }
        }

        return count;
    }

    private List<Node> findClosestNodes(int distance) {

        int closest = 257;
        ArrayList<Node> foundNodes = new ArrayList<>();
        for (var nodeEntry : networkMap.entrySet()) {
            if (Math.abs(nodeEntry.getValue() - distance) < closest){
                closest = nodeEntry.getValue();
                foundNodes.clear();
                foundNodes.add(nodeEntry.getKey());
            } else if (nodeEntry.getValue() == closest && (foundNodes.size() <= 3)) {
                    foundNodes.add(nodeEntry.getKey());
            }
        }

        return foundNodes;
    }

    private void notifyOtherFullNodes() {
        for (Node otherNode : networkMap.keySet()) {
            if (otherNode.nodeName.equals(nodeName)){
                continue;
            }
            String otherNodeName = otherNode.nodeName;
            String otherNodeAddress = otherNode.nodeAddress;
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

    public Map<String, String> getKeyValueStore() {
        return keyValueStore;
    }

    public void disconnectNode() {
        try {
            serverSocket.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public class Node{

        public final String nodeName;
        public final String nodeAddress;

        public Node(String nodeName, String nodeAddress) {
            this.nodeName = nodeName;
            this.nodeAddress = nodeAddress;
        }
    }
}


