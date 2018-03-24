

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;

import java.util.ArrayList;
import java.util.List;

public class EC2Connector {

    private AmazonEC2 ec2;
    private String accessKey;
    private String secretKey;

    public EC2Connector(String awsAccessKey, String awsSecretKey){
        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-west-2")
                .build();

        this.accessKey = awsAccessKey;
        this.secretKey = awsSecretKey;
    }

    public String setNewMachine(String instanceType, String inputQueue, String outputQueue, int imagesPerWorker){
        RunInstancesRequest request = new RunInstancesRequest("ami-d874e0a0", 1, 1);
        request.setInstanceType(InstanceType.T1Micro.toString());
        request.setKeyName("avivor");

        String mutualScript = String.format("#! /bin/bash\n" +
                "echo y|sudo yum install java-1.8.0\n" +
                "echo y|sudo yum remove java-1.7.0-openjdk\n" +
                "mkdir ~/.aws\n" +
                "printf '[default]\n" +
                "aws_access_key_id=%1$s\n" +
                "aws_secret_access_key=%2$s\n' >> ~/.aws/credentials\n" +
                "cd /tmp\n" +
                "export AWS_ACCESS_KEY_ID=%1$s\n" +
                "export AWS_SECRET_ACCESS_KEY=%2$s\n" +
                "sudo aws s3 cp s3://dsps182/code/ . --recursive\n", this.accessKey, this.secretKey);

        String managerScript = mutualScript + String.format("cd manager_jar\n" +
                        "java -jar manager.jar %s %s %s"
                ,inputQueue, outputQueue, imagesPerWorker);

        String workerScript = mutualScript + String.format("cd worker_jar\n" +
                        "java -jar worker.jar %s %s %s"
                ,inputQueue, outputQueue, imagesPerWorker);

        String scriptToApply = "";

        if(instanceType.equals("Manager")){
            scriptToApply = managerScript;
        }
        else if(instanceType.equals("Worker")){
            scriptToApply = workerScript;
        }

        request.setUserData(String.valueOf(com.amazonaws.util.Base64.encodeAsString(scriptToApply.getBytes())));
        List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();

        for (Instance instance : instances) {
            CreateTagsRequest createTagsRequest = new CreateTagsRequest();
            createTagsRequest.withResources(instance.getInstanceId())
                    .withTags(new Tag("Name", instanceType));
            ec2.createTags(createTagsRequest);
        }

        return instances.get(0).getInstanceId();
    }

    public void terminateMachine(String instanceID){
        TerminateInstancesRequest req = new TerminateInstancesRequest();
        List<String> instances = new ArrayList<>();
        instances.add(instanceID);
        req.setInstanceIds(instances);
        ec2.terminateInstances(req);
    }

}
