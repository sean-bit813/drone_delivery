package matcher;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

public class Matcher {

    private static final String DRONES_TABLE = "Drones";
    private static final String ORDERS_TABLE = "Orders";
    private static final String SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/533266960984/Region1Queue";

    private final DynamoDbClient dynamoDB;
    private final SqsClient sqsClient;
    private final ObjectMapper objectMapper;

    public Matcher() {
        dynamoDB = DynamoDbClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
        sqsClient = SqsClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
        objectMapper = new ObjectMapper();
    }

    public static void main(String[] args) {
        Matcher matcher = new Matcher();
        matcher.processMessages();
    }

    public void processMessages() {
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(SQS_QUEUE_URL)
                .maxNumberOfMessages(10)
                .waitTimeSeconds(10)
                .build();

        while (true) {
            ReceiveMessageResponse receiveMessageResponse = sqsClient.receiveMessage(receiveMessageRequest);
            List<Message> messages = receiveMessageResponse.messages();

            for (Message message : messages) {
                try {
                    Map<String, String> snsMessage = objectMapper.readValue(message.body(), Map.class);
                    String orderInfoJson = snsMessage.get("Message");

                    if (orderInfoJson != null) {
                        Map<String, String> orderInfo = objectMapper.readValue(orderInfoJson, Map.class);
                        handleOrderMessage(orderInfo, message.receiptHandle());
                    } else {
                        System.err.println("Order information is missing in the SNS message.");
                        deleteMessage(message.receiptHandle());
                    }
                } catch (Exception e) {
                    System.err.println("Error processing message: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }

    private void handleOrderMessage(Map<String, String> orderInfo, String receiptHandle) {
        String orderId = orderInfo.get("UUID");
        String storeId = orderInfo.get("StoreID");
        String userId = orderInfo.get("UserID");
        String version = orderInfo.get("Version");
        String storeLocation = orderInfo.get("StoreLocation");

        if (orderId == null || orderId.isEmpty() || storeId == null || storeId.isEmpty() || userId == null || userId.isEmpty() || version == null || version.isEmpty() || storeLocation == null || storeLocation.isEmpty()) {
            System.out.println(orderInfo);
            System.out.println("Invalid order data. Deleting message...");
            deleteMessage(receiptHandle);
            return;
        }

        GetItemRequest getOrderRequest = GetItemRequest.builder()
                .tableName(ORDERS_TABLE)
                .key(Map.of("UUID", AttributeValue.builder().s(orderId).build()))
                .build();

        GetItemResponse getOrderResponse = dynamoDB.getItem(getOrderRequest);
        if (getOrderResponse.item() == null || getOrderResponse.item().isEmpty()) {
            System.out.println("Order not found. Deleting message...");
            deleteMessage(receiptHandle);
            return;
        }

        String currentVersion = getOrderResponse.item().get("Version").n();
        if (!currentVersion.equals(version)) {
            System.out.println("Version mismatch. Deleting message...");
            deleteMessage(receiptHandle);
            return;
        }
        Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put("#status", "Status");

        ScanRequest scanRequest = ScanRequest.builder()
                .tableName(DRONES_TABLE)
                .filterExpression("#status = :active")
                .expressionAttributeNames(expressionAttributeNames)
                .expressionAttributeValues(Map.of(":active", AttributeValue.builder().s("ACTIVE").build()))
                .build();

        ScanResponse scanResponse = dynamoDB.scan(scanRequest);
        if (scanResponse.items().isEmpty()) {
            System.out.println("No available drones found.");
            return;
        }

        List<Map<String, AttributeValue>> activeDrones = scanResponse.items();

        Map<String, AttributeValue> nearestDrone = findNearestDrone(storeLocation, activeDrones);
        String droneId = nearestDrone.get("UUID").s();

        if (droneId == null || droneId.isEmpty()) {
            System.out.println("Invalid drone data.");
            return;
        }

        updateOrder(orderId, String.valueOf(Integer.parseInt(version) + 1), droneId);
        updateDrone(droneId);

        System.out.println("Order updated and drone matched. Deleting message...");
        deleteMessage(receiptHandle);
    }

    private Map<String, AttributeValue> findNearestDrone(String storeLocation, List<Map<String, AttributeValue>> drones) {
        String[] storeLocationParts = storeLocation.split(",");
        double storeLat = Double.parseDouble(storeLocationParts[0]);
        double storeLon = Double.parseDouble(storeLocationParts[1]);

        double minDistance = Double.MAX_VALUE;
        List<Map<String, AttributeValue>> nearestDrones = new ArrayList<>();

        for (Map<String, AttributeValue> drone : drones) {
            String[] droneLocationParts = drone.get("Location").s().split(",");
            double droneLat = Double.parseDouble(droneLocationParts[0]);
            double droneLon = Double.parseDouble(droneLocationParts[1]);

            double distance = calculateHaversineDistance(storeLat, storeLon, droneLat, droneLon);

            if (distance < minDistance) {
                minDistance = distance;
                nearestDrones.clear();
                nearestDrones.add(drone);
            } else if (distance == minDistance) {
                nearestDrones.add(drone);
            }
        }

        Random rand = new Random();
        return nearestDrones.get(rand.nextInt(nearestDrones.size()));
    }

    private double calculateHaversineDistance(double lat1, double lon1, double lat2, double lon2) {
        final int R = 6371; // Radius of the Earth in kilometers

        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double distance = R * c; // convert to kilometers

        return distance;
    }

    private void updateOrder(String orderId, String newVersion, String droneId) {
        Map<String, AttributeValueUpdate> updates = new HashMap<>();
        updates.put("Status", AttributeValueUpdate.builder().value(AttributeValue.builder().s("assigned").build()).action(AttributeAction.PUT).build());
        updates.put("AssignedTo", AttributeValueUpdate.builder().value(AttributeValue.builder().s(droneId).build()).action(AttributeAction.PUT).build());
        updates.put("Version", AttributeValueUpdate.builder().value(AttributeValue.builder().n(newVersion).build()).action(AttributeAction.PUT).build());

        UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                .tableName(ORDERS_TABLE)
                .key(Map.of("UUID", AttributeValue.builder().s(orderId).build()))
                .attributeUpdates(updates)
                .build();

        dynamoDB.updateItem(updateRequest);
        System.out.println("Order updated successfully: " + orderId);
    }

    private void updateDrone(String droneId) {
        Map<String, AttributeValueUpdate> updates = new HashMap<>();
        updates.put("Status", AttributeValueUpdate.builder().value(AttributeValue.builder().s("MATCHED").build()).action(AttributeAction.PUT).build());

        UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                .tableName(DRONES_TABLE)
                .key(Map.of("UUID", AttributeValue.builder().s(droneId).build()))
                .attributeUpdates(updates)
                .build();

        dynamoDB.updateItem(updateRequest);
        System.out.println("Drone status updated successfully: " + droneId);
    }

    private void deleteMessage(String receiptHandle) {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(SQS_QUEUE_URL)
                .receiptHandle(receiptHandle)
                .build();
        sqsClient.deleteMessage(deleteMessageRequest);
        System.out.println("Message deleted successfully from SQS.");
    }
}

