package handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;


import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class DroneServiceHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private final DynamoDbClient dynamoDB;
    private final ObjectMapper objectMapper;

    public DroneServiceHandler() {
        dynamoDB = DynamoDbClient.builder()
                .region(Region.US_EAST_1)
                .build();
        objectMapper = new ObjectMapper();
    }

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {
        String httpMethod = request.getHttpMethod();
        String path = request.getPath();
        Map<String, String> pathParameters = request.getPathParameters();

        try {
            switch (httpMethod) {
                case "POST":
                    if ("/drones".equals(path)) {
                        return createDrone(request);
                    }
                    break;
                case "GET":
                    if ("/drones".equals(path) && (pathParameters == null || pathParameters.isEmpty())) {
                        return getDronesByFilter(request);
                    } else if (pathParameters != null && pathParameters.containsKey("drone_id")) {
                        return getDroneByID(pathParameters.get("drone_id"));
                    }
                    break;
                case "DELETE":
                    if (path.startsWith("/drones/")) {
                        return deleteDrone(pathParameters.get("drone_id"));
                    }
                    break;
                default:
                    return new APIGatewayProxyResponseEvent().withStatusCode(405).withBody("Method Not Allowed");
            }
        } catch (Exception e) {
            context.getLogger().log("Error processing request: " + e.toString());
            return new APIGatewayProxyResponseEvent().withStatusCode(500).withBody("Error: " + e.getMessage());
        }

        return new APIGatewayProxyResponseEvent().withStatusCode(400).withBody("Unsupported path");
    }

    private APIGatewayProxyResponseEvent createDrone(APIGatewayProxyRequestEvent event) throws IOException {
        Map<String, String> droneData = objectMapper.readValue(event.getBody(), new TypeReference<Map<String, String>>() {});

        String uuid = UUID.randomUUID().toString();
        PutItemRequest request = PutItemRequest.builder()
                .tableName("Drones")
                .item(Map.of(
                        "UUID", AttributeValue.builder().s(uuid).build(),
                        "Status", AttributeValue.builder().s("ACTIVE").build(),
                        "Location", AttributeValue.builder().s("0,0").build()
                ))
                .build();

        dynamoDB.putItem(request);
        return new APIGatewayProxyResponseEvent().withStatusCode(201).withBody("Drone created successfully with UUID: " + uuid);
    }

    private APIGatewayProxyResponseEvent getDroneByID(String droneId) {
        GetItemRequest request = GetItemRequest.builder()
                .tableName("Drones")
                .key(Map.of("UUID", AttributeValue.builder().s(droneId).build()))
                .build();

        try {
            GetItemResponse response = dynamoDB.getItem(request);

            if (response.item() == null || response.item().isEmpty()) {
                return new APIGatewayProxyResponseEvent()
                        .withStatusCode(404)
                        .withBody("Drone not found");
            } else {
                Map<String, String> simpleAttributes = convertAttributes(response.item());
                String jsonDrone = objectMapper.writeValueAsString(simpleAttributes);
                return new APIGatewayProxyResponseEvent()
                        .withStatusCode(200)
                        .withBody(jsonDrone);
            }
        } catch (DynamoDbException e) {
            return new APIGatewayProxyResponseEvent()
                    .withStatusCode(500)
                    .withBody("Failed to fetch drone: " + e.getMessage());
        } catch (JsonProcessingException e) {
            return new APIGatewayProxyResponseEvent()
                    .withStatusCode(500)
                    .withBody("Error processing drone data: " + e.getMessage());
        }
    }

    private APIGatewayProxyResponseEvent getDronesByFilter(APIGatewayProxyRequestEvent event) {
        Map<String, String> queryParams = event.getQueryStringParameters();
        String statusFilter = queryParams != null ? queryParams.get("status") : null;

        if (statusFilter != null) {
            QueryRequest queryRequest = QueryRequest.builder()
                    .tableName("Drones")
                    .indexName("Status-index")
                    .keyConditionExpression("#st = :statusVal") // Using an alias for 'Status'
                    .expressionAttributeNames(Map.of("#st", "Status")) // Mapping '#st' to 'Status'
                    .expressionAttributeValues(Map.of(":statusVal", AttributeValue.builder().s(statusFilter).build()))
                    .build();

            try {
                var queryResponse = dynamoDB.query(queryRequest);
                List<Map<String, String>> simpleItems = queryResponse.items().stream()
                        .map(this::convertAttributes)
                        .collect(Collectors.toList());
                return new APIGatewayProxyResponseEvent()
                        .withStatusCode(200)
                        .withBody(objectMapper.writeValueAsString(simpleItems));
            } catch (DynamoDbException e) {
                return new APIGatewayProxyResponseEvent()
                        .withStatusCode(500)
                        .withBody("Database Query Error: " + e.getMessage());
            } catch (JsonProcessingException e) {
                return new APIGatewayProxyResponseEvent()
                        .withStatusCode(500)
                        .withBody("Serialization Error: " + e.getMessage());
            }
        } else {
            // If no status filter is provided, scan the entire table (not recommended for large datasets)
            ScanRequest scanRequest = ScanRequest.builder()
                    .tableName("Drones")
                    .build();
            var scanResponse = dynamoDB.scan(scanRequest);
            List<Map<String, String>> simpleItems = scanResponse.items().stream()
                    .map(this::convertAttributes)
                    .collect(Collectors.toList());
            try {
                return new APIGatewayProxyResponseEvent()
                        .withStatusCode(200)
                        .withBody(objectMapper.writeValueAsString(simpleItems));
            } catch (JsonProcessingException e) {
                return new APIGatewayProxyResponseEvent()
                        .withStatusCode(500)
                        .withBody("Serialization Error: " + e.getMessage());
            }
        }
    }

    private APIGatewayProxyResponseEvent deleteDrone(String droneId) {
        GetItemRequest checkRequest = GetItemRequest.builder()
                .tableName("Drones")
                .key(Map.of("UUID", AttributeValue.builder().s(droneId).build()))
                .build();

        try {
            GetItemResponse checkResponse = dynamoDB.getItem(checkRequest);
            if (checkResponse.item() == null || checkResponse.item().isEmpty()) {
                return new APIGatewayProxyResponseEvent()
                        .withStatusCode(404)
                        .withBody("Drone not found for ID: " + droneId);
            } else {
                DeleteItemRequest deleteRequest = DeleteItemRequest.builder()
                        .tableName("Drones")
                        .key(Map.of("UUID", AttributeValue.builder().s(droneId).build()))
                        .build();
                dynamoDB.deleteItem(deleteRequest);
                return new APIGatewayProxyResponseEvent()
                        .withStatusCode(200)
                        .withBody("Drone deleted successfully for ID: " + droneId);
            }
        } catch (DynamoDbException e) {
            return new APIGatewayProxyResponseEvent()
                    .withStatusCode(500)
                    .withBody("Error deleting drone: " + e.getMessage());
        }
    }

    private Map<String, String> convertAttributes(Map<String, AttributeValue> attributes) {
        return attributes.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> attributeValueToString(e.getValue())));
    }

    private String attributeValueToString(AttributeValue value) {
        if (value.s() != null) {
            return value.s();
        } else if (value.n() != null) {
            return value.n();
        } else if (value.bool() != null) {
            return value.bool().toString();
        }
        return null;
    }
}
