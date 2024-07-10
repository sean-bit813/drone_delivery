package kinesis;

import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.lifecycle.events.*;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class DroneRecordProcessor implements ShardRecordProcessor {

    private static final Logger log = LoggerFactory.getLogger(DroneRecordProcessor.class);
    private static final String SHARD_ID_MDC_KEY = "ShardId";
    private static final double EARTH_RADIUS = 6371e3; // Earth radius in meters

    private final DynamoDbAsyncClient dynamoDbClient;
    private final ObjectMapper objectMapper;
    private String shardId;

    public DroneRecordProcessor(DynamoDbAsyncClient dynamoDbClient) {
        this.dynamoDbClient = dynamoDbClient;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
        shardId = initializationInput.shardId();
        log.info("Initializing @ Sequence: {}", initializationInput.extendedSequenceNumber());
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        log.info("Processing {} record(s)", processRecordsInput.records().size());
        for (KinesisClientRecord record : processRecordsInput.records()) {
            try {
                String data = StandardCharsets.UTF_8.decode(record.data()).toString();
                Map<String, String> recordData = objectMapper.readValue(data, Map.class);
                String droneUUID = recordData.get("droneID");
                String geoLocation = recordData.get("location");
                double[] droneLocation = parseGeoLocation(geoLocation);

                findAssignedOrder(droneUUID).thenAccept(assignedOrder -> {
                    if (assignedOrder == null) {
                        log.info("No assigned order found for droneID: {}", droneUUID);
                        return; // No-op if no assigned order
                    }

                    String storeID = assignedOrder.get("StoreID").s();
                    String userID = assignedOrder.get("UserID").s();
                    CompletableFuture<Map<String, AttributeValue>> storeFuture = findLocation("Stores", storeID);
                    CompletableFuture<Map<String, AttributeValue>> userFuture = findLocation("Users", userID);

                    storeFuture.thenCombine(userFuture, (store, user) -> {
                        double[] storeLocation = parseGeoLocation(store.get("Location").s());
                        double[] userLocation = parseGeoLocation(user.get("Location").s());

                        double distanceToStore = calculateHaversineDistance(droneLocation, storeLocation);
                        double distanceToUser = calculateHaversineDistance(droneLocation, userLocation);

                        updateOrderStatus(droneUUID, assignedOrder, distanceToStore, distanceToUser);
                        return null;
                    });
                }).exceptionally(e -> {
                    log.error("Error processing record", e);
                    return null;
                });
            } catch (Exception e) {
                log.error("Error processing record", e);
            }
        }
    }

    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {
        log.info("Lease lost for shard: {}", shardId);
    }

    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
        log.info("Shard ended: {}", shardId);
        try {
            shardEndedInput.checkpointer().checkpoint();
        } catch (ShutdownException | InvalidStateException e) {
            log.error("Exception while checkpointing at shard end. Giving up.", e);
        }
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
        log.info("Scheduler is shutting down, checkpointing.");
        try {
            shutdownRequestedInput.checkpointer().checkpoint();
        } catch (ShutdownException | InvalidStateException e) {
            log.error("Exception while checkpointing at requested shutdown. Giving up.", e);
        }
    }

    private double[] parseGeoLocation(String geoLocation) {
        String[] parts = geoLocation.split(",");
        return new double[]{Double.parseDouble(parts[0]), Double.parseDouble(parts[1])};
    }

    private CompletableFuture<Map<String, AttributeValue>> findAssignedOrder(String droneUUID) {
        log.info("Finding assigned order for droneID: {}", droneUUID);
        QueryRequest queryRequest = QueryRequest.builder()
                .tableName("Orders")
                .indexName("AssignedTo-index")
                .keyConditionExpression("AssignedTo = :droneUUID")
                .expressionAttributeValues(Map.of(":droneUUID", AttributeValue.builder().s(droneUUID).build()))
                .build();
        return dynamoDbClient.query(queryRequest).thenApply(QueryResponse::items)
                .thenApply(items -> {
                    if (items.isEmpty()) {
                        log.info("No assigned order found for droneID: {}", droneUUID);
                        return null;
                    }
                    log.info("Assigned order found for droneID: {}", droneUUID);
                    return items.get(0);
                });
    }

    private CompletableFuture<Map<String, AttributeValue>> findLocation(String tableName, String id) {
        log.info("Finding location for table: {}, ID: {}", tableName, id);
        GetItemRequest getItemRequest = GetItemRequest.builder()
                .tableName(tableName)
                .key(Map.of("UUID", AttributeValue.builder().s(id).build()))
                .build();
        return dynamoDbClient.getItem(getItemRequest).thenApply(response -> {
            log.info("Location found for table: {}, ID: {}", tableName, id);
            return response.item();
        });
    }

    private double calculateHaversineDistance(double[] start, double[] end) {
        double lat1 = Math.toRadians(start[0]);
        double lon1 = Math.toRadians(start[1]);
        double lat2 = Math.toRadians(end[0]);
        double lon2 = Math.toRadians(end[1]);

        double dlat = lat2 - lat1;
        double dlon = lon2 - lon1;

        double a = Math.sin(dlat / 2) * Math.sin(dlat / 2)
                + Math.cos(lat1) * Math.cos(lat2)
                * Math.sin(dlon / 2) * Math.sin(dlon / 2);

        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return EARTH_RADIUS * c;
    }

    private void updateOrderStatus(String droneUUID, Map<String, AttributeValue> assignedOrder, double distanceToStore, double distanceToUser) {
        String orderStatus = assignedOrder.get("Status").s();
        String orderId = assignedOrder.get("UUID").s();

        log.info("Updating order status for orderID: {}", orderId);
        if ("assigned".equals(orderStatus) && distanceToStore < 5) {
            updateOrder(orderId, "PickupCompleted");
            updateDroneStatus(droneUUID, "PickupCompleted");
        } else if ("PickupCompleted".equals(orderStatus) && distanceToUser < 5) {
            updateOrder(orderId, "DropoffCompleted");
        } else if ("DropoffCompleted".equals(orderStatus)) {
            updateOrder(orderId, "Completed");
            updateDroneStatus(droneUUID, "ACTIVE");
        }
    }

    private void updateOrder(String orderId, String newStatus) {
        log.info("Updating orderID: {} to new status: {}", orderId, newStatus);
        UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                .tableName("Orders")
                .key(Map.of("UUID", AttributeValue.builder().s(orderId).build()))
                .attributeUpdates(Map.of("Status", AttributeValueUpdate.builder()
                        .value(AttributeValue.builder().s(newStatus).build())
                        .action(AttributeAction.PUT).build()))
                .build();
        dynamoDbClient.updateItem(updateRequest).thenRun(() -> log.info("OrderID: {} updated to new status: {}", orderId, newStatus));
    }

    private void updateDroneStatus(String droneUUID, String newStatus) {
        log.info("Updating droneID: {} to new status: {}", droneUUID, newStatus);
        UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                .tableName("Drones")
                .key(Map.of("UUID", AttributeValue.builder().s(droneUUID).build()))
                .attributeUpdates(Map.of("Status", AttributeValueUpdate.builder()
                        .value(AttributeValue.builder().s(newStatus).build())
                        .action(AttributeAction.PUT).build()))
                .build();
        dynamoDbClient.updateItem(updateRequest).thenRun(() -> log.info("DroneID: {} updated to new status: {}", droneUUID, newStatus));
    }
}
