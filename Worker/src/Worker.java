
import com.amazonaws.services.sqs.model.Message;
import com.asprise.ocr.Ocr;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;

import static java.lang.Integer.parseInt;

public class Worker {

    private static String process(Ocr ocr, String imageFilename) throws IOException {
        String s = ocr.recognize(new File[] {new File(imageFilename)},
                Ocr.RECOGNIZE_TYPE_ALL, Ocr.OUTPUT_FORMAT_PLAINTEXT); // PLAINTEXT | XML | PDF | RTF
        return s;
    }

    private static String downloadImage(String url) throws IOException {
        URL urlObj = new URL(url);
        String tempDir = System.getProperty("java.io.tmpdir");
        String fileName = url.substring( url.lastIndexOf('/')+1, url.length());
        String outputPath = tempDir + "/" + fileName;
        outputPath = outputPath.replace(",", "__COMMA__");

        InputStream is = null;
        FileOutputStream fos = null;

        try {

            //connect
            HttpURLConnection urlConn = (HttpURLConnection) urlObj.openConnection();
            urlConn.addRequestProperty("User-Agent", "Mozilla/4.76");

            //get inputstream from connection
            is = urlConn.getInputStream();
            fos = new FileOutputStream(outputPath);

            // 4KB buffer
            byte[] buffer = new byte[4096];
            int length;

            // read from source and write into local file
            while ((length = is.read(buffer)) > 0) {
                fos.write(buffer, 0, length);
            }
            return outputPath;
        }
        catch(Exception ex) {
            return null;
        }
        finally {
            try {
                if (is != null) {
                    is.close();
                }
            } finally {
                if (fos != null) {
                    fos.close();
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String inputQueue = args[0];
        String outputQueue = args[1];
        int imagesPerWorker = parseInt(args[2]);

        SQSConnector sqs = new SQSConnector();
        Ocr.setUp(); // one time setup
        Ocr ocr = new Ocr(); // create a new OCR engine
        ocr.startEngine("eng", Ocr.SPEED_FAST); // English

        while(true){
            Message msg = sqs.getMessageByType(inputQueue, "new image task");
            if(msg != null){
                //Download the image file indicated in the message.
                String tmpPath = downloadImage(msg.getBody());

                if(tmpPath != null){
                    //Apply OCR on the image.
                    String text = process(ocr, tmpPath);

                    //Notify the manager of the text associated with that image.
                    //Send "done image task" message to SQS
                    sqs.sendMessage(outputQueue, "done image task", String.format("%s\n%s",msg.getBody(), text));
                }
                else{
                    sqs.sendMessage(outputQueue, "done image task", String.format("%s\n%s",msg.getBody(), "<Could not download image>"));
                }


                //remove the message from the SQS queue
                sqs.deleteMessage(inputQueue, msg);
            }
        }
    }
}
