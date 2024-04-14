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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.time.Instant;

public class OrderServiceHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private final DynamoDbClient dynamoDB;
    private final ObjectMapper objectMapper;

    public OrderServiceHandler() {
        dynamoDB = DynamoDbClient.builder()
                .region(Region.US_EAST_1) // my region
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
                    if (path.equals("/orders")) {
                        return createOrder(request);
                    }
                    break;
                case "GET":
                    if ("/orders".equals(path) && (pathParameters == null || pathParameters.isEmpty())) {
                        return getOrdersByFilter(request);
                    } else if (pathParameters != null && pathParameters.containsKey("order_id")) {
                        return getOrderByID(pathParameters.get("order_id"));
                    }
                    break;
                case "DELETE":
                    if (path.startsWith("/orders/")) {
                        return deleteOrder(request.getPathParameters().get("order_id"));
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

    private APIGatewayProxyResponseEvent createOrder(APIGatewayProxyRequestEvent event) throws IOException {

            Map<String, String> order = objectMapper.readValue(event.getBody(), new TypeReference<Map<String, String>>() {});

            String storeId = order.get("StoreID");
            String userId = order.get("UserID");

            if (!entityExists("Stores", "UUID", storeId)) {
                return new APIGatewayProxyResponseEvent().withStatusCode(404).withBody("Store not found");
            }

            if (!entityExists("Users", "UUID", userId)) {
                return new APIGatewayProxyResponseEvent().withStatusCode(404).withBody("User not found");
            }

            String currentTimestamp = Instant.now().toString();
            PutItemRequest request = PutItemRequest.builder()
                    .tableName("Orders")
                    .item(Map.of(
                            "UUID", AttributeValue.builder().s(UUID.randomUUID().toString()).build(),
                            "StoreID", AttributeValue.builder().s(storeId).build(),
                            "UserID", AttributeValue.builder().s(userId).build(),
                            "CreateAt", AttributeValue.builder().s(currentTimestamp).build()
                    ))
                    .build();

            dynamoDB.putItem(request);
            return new APIGatewayProxyResponseEvent().withStatusCode(201).withBody("Order created successfully");

    }

    private APIGatewayProxyResponseEvent getOrderByID(String orderId) {
        // Attempt to retrieve the order from DynamoDB
        GetItemRequest request = GetItemRequest.builder()
                .tableName("Orders")
                .key(Map.of("UUID", AttributeValue.builder().s(orderId).build()))
                .build();

        try {
            GetItemResponse response = dynamoDB.getItem(request);

            if (response.item() == null || response.item().isEmpty()) {
                return new APIGatewayProxyResponseEvent()
                        .withStatusCode(404)
                        .withBody("Order not found");
            } else {
                // Order exists, serialize it to JSON
                String jsonOrder = objectMapper.writeValueAsString(response.item());
                return new APIGatewayProxyResponseEvent()
                        .withStatusCode(200)
                        .withBody(jsonOrder);
            }
        } catch (DynamoDbException e) {
            return new APIGatewayProxyResponseEvent()
                    .withStatusCode(500)
                    .withBody("Failed to fetch order: " + e.getMessage());
        } catch (JsonProcessingException e) {
            return new APIGatewayProxyResponseEvent()
                    .withStatusCode(500)
                    .withBody("Error processing order data: " + e.getMessage());
        }

    }

    private APIGatewayProxyResponseEvent getOrdersByFilter(APIGatewayProxyRequestEvent event) {
        Map<String, String> queryParams = event.getQueryStringParameters();

        try {
            if (queryParams == null || queryParams.isEmpty()) {
                // Scan the entire table if no query parameters are provided
                ScanRequest scanRequest = ScanRequest.builder()
                        .tableName("Orders")
                        .build();
                var scanResponse = dynamoDB.scan(scanRequest);
                return new APIGatewayProxyResponseEvent()
                        .withStatusCode(200)
                        .withBody(objectMapper.writeValueAsString(scanResponse.items()));
            } else {
                // Build a query based on provided parameters
                String keyConditionExpression = "";
                Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
                String indexName = null;

                if (queryParams.containsKey("UserID")) {
                    keyConditionExpression = "UserID = :userId";
                    expressionAttributeValues.put(":userId", AttributeValue.builder().s(queryParams.get("UserID")).build());
                    //GSI name
                    indexName = "UserID-CreateAt-index";
                }
                if (queryParams.containsKey("StoreID")) {
                    if (!keyConditionExpression.isEmpty()) keyConditionExpression += " and ";
                    keyConditionExpression += "StoreID = :storeId";
                    expressionAttributeValues.put(":storeId", AttributeValue.builder().s(queryParams.get("StoreID")).build());
                    //GSI name
                    indexName = "StoreID-CreateAt-index";
                }

                QueryRequest queryRequest = QueryRequest.builder()
                        .tableName("Orders")
                        .indexName(indexName) // Adjust this based on your DynamoDB GSI settings
                        .keyConditionExpression(keyConditionExpression)
                        .expressionAttributeValues(expressionAttributeValues)
                        .build();

                var queryResponse = dynamoDB.query(queryRequest);
                return new APIGatewayProxyResponseEvent()
                        .withStatusCode(200)
                        .withBody(objectMapper.writeValueAsString(queryResponse.items()));
            }
        } catch (DynamoDbException e) {
            return new APIGatewayProxyResponseEvent()
                    .withStatusCode(500)
                    .withBody("Database Query Error: " + e.getMessage());
        } catch (JsonProcessingException e) {
            return new APIGatewayProxyResponseEvent()
                    .withStatusCode(500)
                    .withBody("Serialization Error: " + e.getMessage());
        }
    }

    private APIGatewayProxyResponseEvent deleteOrder(String orderId) {
        // Create a request to check if the order exists
        GetItemRequest checkRequest = GetItemRequest.builder()
                .tableName("Orders")
                .key(Map.of("UUID", AttributeValue.builder().s(orderId).build()))
                .build();

        try {
            // Check if the order exists
            GetItemResponse checkResponse = dynamoDB.getItem(checkRequest);
            if (checkResponse.item() == null || checkResponse.item().isEmpty()) {
                // Order not found, return an error response
                return new APIGatewayProxyResponseEvent()
                        .withStatusCode(404)
                        .withBody("Order not found for ID: " + orderId);
            } else {
                // Order exists, proceed with deletion
                DeleteItemRequest deleteRequest = DeleteItemRequest.builder()
                        .tableName("Orders")
                        .key(Map.of("UUID", AttributeValue.builder().s(orderId).build()))
                        .build();
                dynamoDB.deleteItem(deleteRequest);

                // Return a success response
                return new APIGatewayProxyResponseEvent()
                        .withStatusCode(200)
                        .withBody("Order deleted successfully for ID: " + orderId);
            }
        } catch (DynamoDbException e) {
            // Handle potential database errors
            return new APIGatewayProxyResponseEvent()
                    .withStatusCode(500)
                    .withBody("Error deleting order: " + e.getMessage());
        }
    }

    // helper function to check entity exist
    private boolean entityExists(String tableName, String keyName, String keyValue) {
        GetItemRequest request = GetItemRequest.builder()
                .tableName(tableName)
                .key(Map.of(keyName, AttributeValue.builder().s(keyValue).build()))
                .build();

        try {
            GetItemResponse response = dynamoDB.getItem(request);
            return response.item() != null && !response.item().isEmpty();
        } catch (DynamoDbException e) {
            throw new RuntimeException("Database Error: " + e.getMessage(), e);
        }
    }
}

