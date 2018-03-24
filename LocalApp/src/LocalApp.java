import com.amazonaws.services.sqs.model.Message;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.lang.Integer.parseInt;

public class LocalApp {
    public static void main(String[] args) throws IOException, InterruptedException {
        String inputFile = args[0];
        int imagesPerWorker = parseInt(args[1]);

        //TODO: Before execution, set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables and AWS_PROFILE=dsps on Aviv's machine
        String awsAccessKey = System.getenv("AWS_ACCESS_KEY_ID");
        String awsSecretKey = System.getenv("AWS_SECRET_ACCESS_KEY");

        EC2Connector ec2 = new EC2Connector(awsAccessKey, awsSecretKey);
        SQSConnector sqs = new SQSConnector();
        S3Connector s3 = new S3Connector();

        //Create SQS
        String inputQueue = sqs.createQueue("input_");
        String outputQueue = sqs.createQueue("output_");


        //Check if Manager is active, If not, start the manager
        String managerInstanceID = ec2.setNewMachine("Manager", inputQueue, outputQueue, imagesPerWorker);

        //Upload the urls file to S3
        s3.uploadObject(inputFile, "dsps182");

        //Send "new task" message to SQS with the S3 path of the file
        File f = new File(inputFile);
        sqs.sendMessage(inputQueue, "new task", f.getName());

        //Wait for "done" message
        while(true){
            Message msg = sqs.getMessageByType(outputQueue,"done task");
            if(msg != null){
                System.out.println("Found 'done task' message!");
                //Download response from S3
                String outputFileName = msg.getBody();
                List<String> lines = s3.downloadObject(outputFileName, "dsps182", true);
                for (int i=0; i<lines.size(); i++)
                {
                    System.out.println(lines.get(i));
                }
                break;
            }
            else{
                TimeUnit.SECONDS.sleep(10);
            }
        }


        sqs.deleteQueue(inputQueue);
        sqs.deleteQueue(outputQueue);
        ec2.terminateMachine(managerInstanceID);

    }
}
