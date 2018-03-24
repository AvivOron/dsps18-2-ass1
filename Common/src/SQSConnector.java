import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.internal.SdkInternalMap;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;

import java.util.*;

public class SQSConnector {

    private AmazonSQS sqs;

    public SQSConnector(){
        AWSCredentialsProvider credentialsProvider =
                new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        sqs = AmazonSQSClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-west-2")
                .build();
    }

    public String createQueue(String queueName){
        System.out.println("Creating a new SQS queue.\n");
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName + UUID.randomUUID());
        String queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        return queueUrl;
    }

    public void sendMessage(String queueUrl, String messageType, String messageBody){
        System.out.println("Sending a message to SQS.\n");
        SendMessageRequest req = new SendMessageRequest(queueUrl, messageBody);
        MessageAttributeValue msgAttrVal = new MessageAttributeValue();
        msgAttrVal.setStringValue(messageType);
        msgAttrVal.withDataType("String");
        req.addMessageAttributesEntry("type", msgAttrVal);
        sqs.sendMessage(req);
    }

    public Message getMessageByType(String queueUrl, String messageType){
        System.out.println("Trying to get message from SQS.\n");
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
        receiveMessageRequest.withMessageAttributeNames("type");
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        for (Message message : messages) {
            for (Map.Entry<String, MessageAttributeValue> entry : message.getMessageAttributes().entrySet()) {
                if(entry.getKey().equals("type") && entry.getValue().getStringValue().equals(messageType)){
                    return message;
                }
            }
        }

        return null;
    }

    public void deleteMessage(String queueUrl, Message message){
        System.out.println("Deleting a message.\n");
        String messageRecieptHandle = message.getReceiptHandle();
        sqs.deleteMessage(new DeleteMessageRequest(queueUrl, messageRecieptHandle));
    }

    public void deleteQueue(String queueUrl){
        // Delete a queue
        System.out.println("Deleting the queue.\n");
        sqs.deleteQueue(new DeleteQueueRequest(queueUrl));
    }
}