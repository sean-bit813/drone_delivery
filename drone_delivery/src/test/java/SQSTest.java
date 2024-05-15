import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;

import static org.junit.Assert.*;

public class SQSTest {

    private SqsClient sqs;
    private String queueUrl;

    @Before
    public void setUp() {
        sqs = SqsClient.builder()
                .region(Region.US_EAST_1) // Set your region
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
        queueUrl = "https://sqs.us-east-1.amazonaws.com/533266960984/testQueue1"; // Change to your test queue URL
    }

    @After
    public void tearDown() {
        // Clean up the queue by deleting the messages
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(10)
                .build();
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();
        for (Message message : messages) {
            sqs.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build());
        }
    }

    @Test
    public void testSendMessage() {
        String messageBody = "Hello World";
        SendMessageResponse sendMsgResponse = sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .build());
        assertNotNull(sendMsgResponse.messageId());
    }

    @Test
    public void testReceiveMessage() {
        String sentMessageBody = "Test Message";
        sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(sentMessageBody)
                .build());
        List<Message> messages = sqs.receiveMessage(ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(1)
                .build()).messages();

        assertFalse(messages.isEmpty());
        assertEquals(sentMessageBody, messages.get(0).body());
    }

    @Test
    public void testDeleteMessage() {
        // Sending message
        String messageBody = "Message for Deletion";
        SendMessageResponse sendMsgResponse = sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .build());
        assertNotNull(sendMsgResponse.messageId());

        // Receiving message
        ReceiveMessageResponse receiveMessageResponse = sqs.receiveMessage(ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(1)
                .build());
        assertFalse(receiveMessageResponse.messages().isEmpty());

        Message receivedMessage = receiveMessageResponse.messages().get(0);
        assertNotNull(receivedMessage.receiptHandle());

        // Deleting message
        sqs.deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(receivedMessage.receiptHandle())
                .build());

        // Confirm deletion by attempting to receive the message again
        receiveMessageResponse = sqs.receiveMessage(ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(1)
                .waitTimeSeconds(10)
                .build());
        assertTrue(receiveMessageResponse.messages().isEmpty());
    }
}
