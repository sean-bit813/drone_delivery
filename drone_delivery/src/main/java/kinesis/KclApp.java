package kinesis;

import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.KinesisClientUtil;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.retrieval.polling.PollingConfig;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import java.util.UUID;
import java.util.concurrent.Executors;

public class KclApp {

    private static final String STREAM_NAME = "dronelocation1";
    private static final String REGION = "us-east-1";
    private static final String APPLICATION_NAME = "DroneDeliveryApp";
    private static final String WORKER_ID = "worker-" + UUID.randomUUID().toString();

    public static void main(String[] args) {
        System.out.println("Starting KCL application...");

        KinesisAsyncClient kinesisClient = KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder().region(Region.of(REGION)));

        DynamoDbAsyncClient dynamoDbClient = DynamoDbAsyncClient.builder()
                .region(Region.of(REGION))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        CloudWatchAsyncClient cloudWatchClient = CloudWatchAsyncClient.builder()
                .region(Region.of(REGION))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        DroneRecordProcessorFactory recordProcessorFactory = new DroneRecordProcessorFactory(dynamoDbClient);

        ConfigsBuilder configsBuilder = new ConfigsBuilder(
                STREAM_NAME,
                APPLICATION_NAME,
                kinesisClient,
                dynamoDbClient,
                cloudWatchClient,
                WORKER_ID,
                recordProcessorFactory);

        Scheduler scheduler = new Scheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig().metricsLevel(MetricsLevel.DETAILED),
                configsBuilder.processorConfig(),
                configsBuilder.retrievalConfig().retrievalSpecificConfig(new PollingConfig(STREAM_NAME, kinesisClient))
        );

        Executors.newSingleThreadExecutor().execute(() -> {
            System.out.println("Scheduler started.");
            scheduler.run();
        });
    }
}
