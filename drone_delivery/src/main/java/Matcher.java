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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
                    Map<String, String> orderInfo = objectMapper.readValue(message.body(), Map.class);
                    handleOrderMessage(orderInfo, message.receiptHandle());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void handleOrderMessage(Map<String, String> orderInfo, String receiptHandle) {
        String orderId = orderInfo.get("UUID");
        String storeId = orderInfo.get("StoreID");
        String userId = orderInfo.get("UserID");
        String region = orderInfo.get("Region");
        int version = Integer.parseInt(orderInfo.get("Version"));

        GetItemRequest getOrderRequest = GetItemRequest.builder()
                .tableName(ORDERS_TABLE)
                .key(Map.of("UUID", AttributeValue.builder().s(orderId).build()))
                .build();

        GetItemResponse getOrderResponse = dynamoDB.getItem(getOrderRequest);
        if (getOrderResponse.item() == null || getOrderResponse.item().isEmpty()) {
            deleteMessage(receiptHandle);
            return;
        }

        int currentVersion = Integer.parseInt(getOrderResponse.item().get("Version").n());
        if (currentVersion != version) {
            deleteMessage(receiptHandle);
            return;
        }

        ScanRequest scanRequest = ScanRequest.builder()
                .tableName(DRONES_TABLE)
                .filterExpression("Status = :active")
                .expressionAttributeValues(Map.of(":active", AttributeValue.builder().s("ACTIVE").build()))
                .build();

        ScanResponse scanResponse = dynamoDB.scan(scanRequest);
        if (scanResponse.items().isEmpty()) {
            return;
        }

        Map<String, AttributeValue> drone = scanResponse.items().get(0);
        String droneId = drone.get("UUID").s();

        updateOrder(orderId, version + 1, droneId);
        updateDrone(droneId);

        deleteMessage(receiptHandle);
    }

    private void updateOrder(String orderId, int newVersion, String droneId) {
        Map<String, AttributeValueUpdate> updates = new HashMap<>();
        updates.put("Status", AttributeValueUpdate.builder().value(AttributeValue.builder().s("assigned").build()).action(AttributeAction.PUT).build());
        updates.put("AssignedTo", AttributeValueUpdate.builder().value(AttributeValue.builder().s(droneId).build()).action(AttributeAction.PUT).build());
        updates.put("Version", AttributeValueUpdate.builder().value(AttributeValue.builder().n(String.valueOf(newVersion)).build()).action(AttributeAction.PUT).build());

        UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                .tableName(ORDERS_TABLE)
                .key(Map.of("UUID", AttributeValue.builder().s(orderId).build()))
                .attributeUpdates(updates)
                .build();

        dynamoDB.updateItem(updateRequest);
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
    }

    private void deleteMessage(String receiptHandle) {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(SQS_QUEUE_URL)
                .receiptHandle(receiptHandle)
                .build();
        sqsClient.deleteMessage(deleteMessageRequest);
    }
}
