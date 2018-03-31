import com.amazonaws.services.sqs.model.Message;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import static java.lang.Integer.parseInt;

public class Manager {
    public static void main(String[] args) throws IOException, InterruptedException {

        String inputQueue = args[0];
        String outputQueue = args[1];
        int imagesPerWorker = parseInt(args[2]);
        String awsAccessKey = System.getenv("AWS_ACCESS_KEY_ID");
        String awsSecretKey = System.getenv("AWS_SECRET_ACCESS_KEY");

        SQSConnector sqs = new SQSConnector();
        S3Connector s3 = new S3Connector();
        EC2Connector ec2 = new EC2Connector(awsAccessKey,awsSecretKey);

        while(true){
            //Look for "new task" message on SQS and pop it
            Message msg = sqs.getMessageByType(inputQueue,"new task");
            if(msg != null){
                sqs.deleteMessage(inputQueue, msg);

                //Download the image list from S3
                List<String> lines = s3.downloadObject(msg.getBody(), "dsps182", false);
                lines.removeAll(Arrays.asList("", null)); //clear empty lines
                lines = new ArrayList(new HashSet(lines)); //remove duplications

                //Create "new image task" message in SQS for each line the file
                for (int i=0; i<lines.size(); i++)
                {
                    String line = lines.get(i);
                    if(line.startsWith("http") || line.startsWith("www")){
                        sqs.sendMessage(inputQueue, "new image task", line);
                    }
                    else{
                        lines.remove(i);
                    }
                }

                //AmazonEC2Exception: You have requested more instances (21) than your current instance limit of 20 allows for the specified instance type
                int numOfWorkers = (int) Math.ceil((double)lines.size()/imagesPerWorker);
                if(numOfWorkers > 19){
                    System.out.println(String.format("Cant create %d workers (max=19). Creating 19 Workers...\n", numOfWorkers));
                }

                //Create (num_of_links)/(images_per_worker) worker nodes
                List<String> workersIds = new ArrayList<>();
                for(int i=0; i<Math.min(numOfWorkers, 19); i++){
                    String workerInstanceID = ec2.setNewMachine("Worker", inputQueue, outputQueue, imagesPerWorker);
                    workersIds.add(workerInstanceID);
                }

                //When the image queue is 0, read all "done image task" messages
                HashMap<String,String> hm=new HashMap<String,String>();
                while(hm.size() < lines.size()) {
                    Message doneImageMsg = sqs.getMessageByType(outputQueue, "done image task");

                    if (doneImageMsg != null) {
                        String fileName = doneImageMsg.getBody().substring(0,doneImageMsg.getBody().indexOf("\n"));

                        String data;
                        try{
                            data = doneImageMsg.getBody().substring(doneImageMsg.getBody().indexOf("\n") + 1, doneImageMsg.getBody().length() - 1);
                        }
                        catch(Exception ex){
                            data = String.format("--> Could'nt fetch text from image on path: %s", doneImageMsg.getBody());
                        }

                        try{
                            hm.put(fileName, data);
                        }
                        catch(Exception ex){
                            System.out.println(ex.getMessage());
                        }

                        sqs.deleteMessage(outputQueue, doneImageMsg);
                    }
                }

                //Create HTML output file
                File temp = File.createTempFile("results", ".html");

                String htmlTemplatePrefix = "<html>\n" +
                        "<body>\n";

                String htmlTemplateSuffix = "</body>\n" +
                        "</html>";

                FileWriter fw = new FileWriter(temp, true);
                fw.write(htmlTemplatePrefix);

                for (Map.Entry<String, String> entry : hm.entrySet()) {
                    String imageUrl = entry.getKey();
                    String text = entry.getValue();
                    fw.write(String.format("<img src='%s'/ style='max-width: 600px;'>\n<br>\n", imageUrl));
                    fw.write(String.format("%s\n<br><br>\n", text));
                }

                fw.write(htmlTemplateSuffix);
                fw.close();

                //Upload the output file to S3
                s3.uploadObject(temp.getAbsolutePath(),"dsps182");
                String tmpFileName = temp.getName();
                temp.delete();


                //Send a "done task" message to SQS with the S3 path
                sqs.sendMessage(outputQueue, "done task", tmpFileName);

                for(int i=0; i<workersIds.size(); i++) {
                    ec2.terminateMachine(workersIds.get(i));
                }

                break;
            }
        }
    }
}
