import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class S3Connector {

    private AmazonS3 s3;

    public S3Connector(){
        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-west-2")
                .build();
    }

    public void uploadObject(String fileName, String bucketName){
        System.out.println("Uploading a new object to S3 from a file\n");
        File file = new File(fileName);
        String key = file.getName().replace('\\', '_').replace('/','_').replace(':', '_');
        PutObjectRequest req = new PutObjectRequest(bucketName, key, file);
        s3.putObject(req);
    }

    public List<String> downloadObject(String fileName, String bucketName, Boolean saveToLocal) throws IOException {
        System.out.println("Downloading an object");
        S3Object object = s3.getObject(new GetObjectRequest(bucketName, fileName));
        System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
        if(saveToLocal){
            return saveObjectToLocal(object.getObjectContent());
        }
        else {
            return displayTextToScreen(object.getObjectContent());
        }
    }

    private static List<String> saveObjectToLocal(InputStream input) throws IOException {

        File temp = File.createTempFile("results", ".html");
        FileWriter fw = new FileWriter(temp, true);
        List<String> lst = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null) break;
            fw.write(line);
        }
        fw.close();
        lst.add(temp.getAbsolutePath());
        return lst;
    }

    private static List<String> displayTextToScreen(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        List<String> lst = new ArrayList<>();
        while (true) {
            String line = reader.readLine();
            if (line == null) break;
            lst.add(line);
        }

        return lst;
    }

}


