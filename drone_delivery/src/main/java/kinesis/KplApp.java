package kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class KplApp {

    private static final String STREAM_NAME = "dronelocation1";
    private static final String REGION = "us-east-1";
    private static final String DRONES_TABLE = "Drones";
    private static final String ORDERS_TABLE = "Orders";
    private static final String STORES_TABLE = "Stores";
    private static final String USERS_TABLE = "Users";

    private static final Random RANDOM = new Random();
    private static final double MOVEMENT_RANGE = 5; // Adjust this value for movement range

    private final KinesisProducer producer;
    private final DynamoDbClient dynamoDB;
    private final ObjectMapper objectMapper;

    // Map to store the last known location of each drone
    private final Map<String, String> droneLocations;

    public KplApp() {
        KinesisProducerConfiguration config = new KinesisProducerConfiguration();
        config.setRegion(REGION);
        config.setMaxConnections(1);
        config.setRequestTimeout(60000);
        config.setRecordMaxBufferedTime(2000); // 2 seconds

        AWSCredentialsProvider credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();
        config.setCredentialsProvider(credentialsProvider);

        producer = new KinesisProducer(config);

        dynamoDB = DynamoDbClient.builder()
                .region(Region.of(REGION))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        objectMapper = new ObjectMapper();
        droneLocations = new HashMap<>();
    }

    public static void main(String[] args) {
        KplApp app = new KplApp();
        app.start();
    }

    public void start() {
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                processDrones();
            }
        }, 0, 5000); // Adjust this interval as needed
    }

    private void processDrones() {
        // Scan the Drones table for all drones
        ScanRequest scanRequest = ScanRequest.builder()
                .tableName(DRONES_TABLE)
                .build();

        ScanResponse scanResponse = dynamoDB.scan(scanRequest);
        List<Map<String, AttributeValue>> drones = scanResponse.items();

        for (Map<String, AttributeValue> drone : drones) {
            String droneID = drone.get("UUID").s();
            String status = drone.get("Status").s();

            updateDroneLocation(droneID, status);
        }
    }

    private void updateDroneLocation(String droneID, String status) {
        try {
            String currentLocation = droneLocations.getOrDefault(droneID, null);

            // If we don't have a last known location in the map, fetch it from DynamoDB
            if (currentLocation == null) {
                GetItemRequest getRequest = GetItemRequest.builder()
                        .tableName(DRONES_TABLE)
                        .key(Map.of("UUID", AttributeValue.builder().s(droneID).build()))
                        .build();

                GetItemResponse getResponse = dynamoDB.getItem(getRequest);
                Map<String, AttributeValue> item = getResponse.item();

                if (item != null && !item.isEmpty()) {
                    currentLocation = item.get("Location").s();
                    droneLocations.put(droneID, currentLocation);
                } else {
                    System.out.println("Drone not found: " + droneID);
                    return;
                }
            }

            String newLocation = calculateNewLocation(currentLocation, status, droneID);
            sendLocationUpdate(droneID, newLocation);
            droneLocations.put(droneID, newLocation);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String calculateNewLocation(String currentLocation, String status, String droneID) {
        String[] parts = currentLocation.split(",");
        double lat = Double.parseDouble(parts[0]);
        double lon = Double.parseDouble(parts[1]);

        if ("ACTIVE".equals(status)) {
            // Simulate random movement
            lat += (RANDOM.nextDouble() - 0.5) * MOVEMENT_RANGE;
            lon += (RANDOM.nextDouble() - 0.5) * MOVEMENT_RANGE;
        } else {
            String targetLocation = getTargetLocation(status, droneID);
            if (targetLocation != null) {
                String[] targetParts = targetLocation.split(",");
                double targetLat = Double.parseDouble(targetParts[0]);
                double targetLon = Double.parseDouble(targetParts[1]);

                lat = moveTowards(lat, targetLat);
                lon = moveTowards(lon, targetLon);
            }
        }

        return lat + "," + lon;
    }

    private String getTargetLocation(String status, String droneID) {
        QueryRequest queryRequest = QueryRequest.builder()
                .tableName(ORDERS_TABLE)
                .indexName("AssignedTo-index")
                .keyConditionExpression("AssignedTo = :droneID")
                .expressionAttributeValues(Map.of(":droneID", AttributeValue.builder().s(droneID).build()))
                .build();

        QueryResponse queryResponse = dynamoDB.query(queryRequest);
        List<Map<String, AttributeValue>> orders = queryResponse.items();

        if (orders.isEmpty()) {
            return null;
        }

        Map<String, AttributeValue> order = orders.get(0);
        String storeID = order.get("StoreID").s();
        String userID = order.get("UserID").s();

        if ("MATCHED".equals(status)) {
            return getLocation(STORES_TABLE, storeID);
        } else if ("PickupCompleted".equals(status)) {
            return getLocation(USERS_TABLE, userID);
        }

        return null;
    }

    private String getLocation(String tableName, String id) {
        GetItemRequest getRequest = GetItemRequest.builder()
                .tableName(tableName)
                .key(Map.of("UUID", AttributeValue.builder().s(id).build()))
                .build();

        GetItemResponse getResponse = dynamoDB.getItem(getRequest);
        Map<String, AttributeValue> item = getResponse.item();

        if (item != null && !item.isEmpty()) {
            return item.get("Location").s();
        }

        return null;
    }

    private double moveTowards(double current, double target) {
        if (current < target) {
            current += MOVEMENT_RANGE;
            if (current > target) {
                current = target;
            }
        } else {
            current -= MOVEMENT_RANGE;
            if (current < target) {
                current = target;
            }
        }
        return current;
    }

    private void sendLocationUpdate(String droneID, String newLocation) {
        try {
            ObjectNode locationUpdate = objectMapper.createObjectNode();
            locationUpdate.put("droneID", droneID);
            locationUpdate.put("location", newLocation);

            String locationJson = objectMapper.writeValueAsString(locationUpdate);
            ByteBuffer data = ByteBuffer.wrap(locationJson.getBytes());

            // Hash the partition key to distribute across shards
            String partitionKey = hashPartitionKey(droneID);

            // Add detailed logging to track partition key and data
            System.out.println("Sending location update - Partition Key: " + partitionKey + ", Data: " + locationJson);

            // Use hashed partition key to ensure better distribution
            producer.addUserRecord(STREAM_NAME, partitionKey, data);
            System.out.println("Sent location update for " + droneID + ": " + newLocation);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String hashPartitionKey(String partitionKey) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(partitionKey.getBytes(StandardCharsets.UTF_8));
            return bytesToHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("No such algorithm exception for SHA-256", e);
        }
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();
        for (byte b : bytes) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) hexString.append('0');
            hexString.append(hex);
        }
        return hexString.toString();
    }
}










