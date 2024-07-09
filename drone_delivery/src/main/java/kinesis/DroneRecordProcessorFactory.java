package kinesis;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

public class DroneRecordProcessorFactory implements ShardRecordProcessorFactory {

    private final DynamoDbAsyncClient dynamoDbClient;

    public DroneRecordProcessorFactory(DynamoDbAsyncClient dynamoDbClient) {
        this.dynamoDbClient = dynamoDbClient;
    }

    @Override
    public ShardRecordProcessor shardRecordProcessor() {
        return new DroneRecordProcessor(dynamoDbClient);
    }
}
