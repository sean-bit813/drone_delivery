import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class SQSTest {

    private AmazonSQS sqs;
    private String queueUrl;

    @Before
    public void setUp() {
        sqs = AmazonSQSClientBuilder.standard().withRegion("us-east-1").build();  // Set your region
        queueUrl = "https://sqs.us-east-1.amazonaws.com/533266960984/testQueue1"; // Change to your test queue URL
    }

    @After
    public void tearDown() {
        // Clean up the queue by deleting the messages
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest.withMaxNumberOfMessages(10)).getMessages();
        for (Message message : messages) {
            sqs.deleteMessage(new DeleteMessageRequest(queueUrl, message.getReceiptHandle()));
        }
    }

    @Test
    public void testSendMessage() {
        String messageBody = "Hello World";
        SendMessageResult sendMsgResult = sqs.sendMessage(new SendMessageRequest(queueUrl, messageBody));
        assertNotNull(sendMsgResult.getMessageId());
    }

    @Test
    public void testReceiveMessage() {
        String sentMessageBody = "Test Message";
        sqs.sendMessage(new SendMessageRequest(queueUrl, sentMessageBody));
        List<Message> messages = sqs.receiveMessage(new ReceiveMessageRequest(queueUrl).withMaxNumberOfMessages(1)).getMessages();

        assertFalse(messages.isEmpty());
        assertEquals(sentMessageBody, messages.get(0).getBody());
    }

    @Test
    public void testDeleteMessage() {
        // Sending message
       // SendMessageResult sendMsgResult = sqs.sendMessage(new SendMessageRequest(queueUrl, "Message for Deletion"));
       // assertNotNull(sendMsgResult.getMessageId());

        // Receiving message
        ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(new ReceiveMessageRequest(queueUrl).withMaxNumberOfMessages(1));
        assertFalse(receiveMessageResult.getMessages().isEmpty());

        Message receivedMessage = receiveMessageResult.getMessages().get(0);
        assertNotNull(receivedMessage.getReceiptHandle());

        // Deleting message
        DeleteMessageRequest deleteRequest = new DeleteMessageRequest(queueUrl, receivedMessage.getReceiptHandle());
        sqs.deleteMessage(deleteRequest);

        // Confirm deletion by attempting to receive the message again
        receiveMessageResult = sqs.receiveMessage(new ReceiveMessageRequest(queueUrl).withMaxNumberOfMessages(1).withWaitTimeSeconds(10));
        assertTrue(receiveMessageResult.getMessages().isEmpty());
    }

}
