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
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.time.Instant;
import java.util.stream.Collectors;

public class OrderServiceHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private final DynamoDbClient dynamoDB;
    private final SnsClient snsClient;
    private final ObjectMapper objectMapper;
    private final String snsTopicArn = "arn:aws:sns:us-east-1:533266960984:OrderTopic"; // SNS Topic ARN

    public OrderServiceHandler() {
        dynamoDB = DynamoDbClient.builder()
                .region(Region.US_EAST_1) // my region
                .build();
        snsClient = SnsClient.builder()
                .region(Region.US_EAST_1) // my region
                .build();
        objectMapper = new ObjectMapper();
    }

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {
        String httpMethod = request.getHttpMethod();
        String path = request.getPath();
        Map<String, String> pathParameters = request.getPathParameters();
        context.getLogger().log("Received input: " + request.getBody());

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

        // Fetch region from the Stores table
        String region = getStoreRegion(storeId);
        if (region == null) {
            return new APIGatewayProxyResponseEvent().withStatusCode(500).withBody("Failed to fetch store region");
        }

        String currentTimestamp = Instant.now().toString();
        String orderId = UUID.randomUUID().toString();
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("UUID", AttributeValue.builder().s(orderId).build());
        item.put("StoreID", AttributeValue.builder().s(storeId).build());
        item.put("UserID", AttributeValue.builder().s(userId).build());
        item.put("CreateAt", AttributeValue.builder().s(currentTimestamp).build());
        item.put("Status", AttributeValue.builder().s("created").build());
        item.put("AssignedTo", AttributeValue.builder().s("").build());
        item.put("Version", AttributeValue.builder().n("1").build());

        PutItemRequest request = PutItemRequest.builder()
                .tableName("Orders")
                .item(item)
                .build();

        dynamoDB.putItem(request);

        // Push the order info to SNS
        Map<String, String> orderInfo = new HashMap<>();
        orderInfo.put("UUID", orderId);
        orderInfo.put("StoreID", storeId);
        orderInfo.put("UserID", userId);
        orderInfo.put("Status", "created");
        orderInfo.put("AssignedTo", "");
        orderInfo.put("Version", "1");

        Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
        messageAttributes.put("Region", MessageAttributeValue.builder()
                .dataType("String")
                .stringValue(region)
                .build());

        String message = objectMapper.writeValueAsString(orderInfo);
        PublishRequest publishRequest = PublishRequest.builder()
                .topicArn(snsTopicArn)
                .message(message)
                .messageAttributes(messageAttributes)
                .build();

        PublishResponse publishResponse = snsClient.publish(publishRequest);

        if (publishResponse.sdkHttpResponse().isSuccessful()) {
            return new APIGatewayProxyResponseEvent().withStatusCode(201).withBody("Order created and published successfully");
        } else {
            return new APIGatewayProxyResponseEvent().withStatusCode(500).withBody("Order created but failed to publish to SNS");
        }
    }

    private String getStoreRegion(String storeId) {
        GetItemRequest request = GetItemRequest.builder()
                .tableName("Stores")
                .key(Map.of("UUID", AttributeValue.builder().s(storeId).build()))
                .build();

        try {
            GetItemResponse response = dynamoDB.getItem(request);
            if (response.item() == null || response.item().isEmpty()) {
                return null;
            } else {
                return response.item().get("Region").s();
            }
        } catch (DynamoDbException e) {
            return null;
        }
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
                Map<String, String> simpleAttributes = convertAttributes(response.item());
                String jsonOrder = objectMapper.writeValueAsString(simpleAttributes);
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
                List<Map<String, String>> simpleItems = scanResponse.items().stream()
                        .map(this::convertAttributes)
                        .collect(Collectors.toList());
                return new APIGatewayProxyResponseEvent()
                        .withStatusCode(200)
                        .withBody(objectMapper.writeValueAsString(simpleItems));
            } else {
                // Build a query based on provided parameters
                String keyConditionExpression = "";
                Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
                String indexName = null;

                if (queryParams.containsKey("UserID")) {
                    keyConditionExpression = "UserID = :userId";
                    expressionAttributeValues.put(":userId", AttributeValue.builder().s(queryParams.get("UserID")).build());
                    indexName = "UserID-CreateAt-index";
                }
                if (queryParams.containsKey("StoreID")) {
                    if (!keyConditionExpression.isEmpty()) keyConditionExpression += " and ";
                    keyConditionExpression += "StoreID = :storeId";
                    expressionAttributeValues.put(":storeId", AttributeValue.builder().s(queryParams.get("StoreID")).build());
                    indexName = "StoreID-CreateAt-index";
                }

                QueryRequest queryRequest = QueryRequest.builder()
                        .tableName("Orders")
                        .indexName(indexName)
                        .keyConditionExpression(keyConditionExpression)
                        .expressionAttributeValues(expressionAttributeValues)
                        .build();

                var queryResponse = dynamoDB.query(queryRequest);
                List<Map<String, String>> simpleItems = queryResponse.items().stream()
                        .map(this::convertAttributes)
                        .collect(Collectors.toList());
                return new APIGatewayProxyResponseEvent()
                        .withStatusCode(200)
                        .withBody(objectMapper.writeValueAsString(simpleItems));
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

    private String attributeValueToString(AttributeValue value) {
        // This method converts AttributeValue to a String for simplicity.
        // You can extend this to handle different types (N, B, SS, etc.) as needed.
        if (value.s() != null) {
            return value.s();
        } else if (value.n() != null) {
            return value.n();
        } else if (value.bool() != null) {
            return value.bool().toString();
        }
        return null; // or handle other types as necessary
    }

    private Map<String, String> convertAttributes(Map<String, AttributeValue> attributes) {
        return attributes.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> attributeValueToString(e.getValue())));
    }
}


