package com.james.amazon.s3;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.james.demo.compress.CompressedFileReader;

public class S3RawLogReader {

    public static void main(String[] args) throws Exception {

        /*
         * The ProfileCredentialsProvider will return your [default] credential profile
         * by reading from the credentials file located at (~/.aws/credentials).
         */
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
                    + "Please make sure that your credentials file is at the correct "
                    + "location (~/.aws/credentials), and is in valid format.", e);
        }

        AmazonS3 s3 = new AmazonS3Client(credentials);
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        s3.setRegion(usEast1);

        String bucketName = "logsheddev";
        String key = "draft.csv";

        System.out.println("===========================================");
        System.out.println("Getting Started with Amazon S3");
        System.out.println("===========================================\n");

        try {
            // /*
            // * List the buckets in your account
            // */
            // System.out.println("Listing buckets");
            // for (Bucket bucket : s3.listBuckets()) {
            // System.out.println(" - " + bucket.getName());
            // }
            // System.out.println();

            /*
             * List objects in your bucket by prefix - There are many options for listing
             * the objects in your bucket. Keep in mind that buckets with many objects might
             * truncate their results when listing their objects, so be sure to check if the
             * returned object listing is truncated, and use the
             * AmazonS3.listNextBatchOfObjects(...) operation to retrieve additional
             * results.
             */
            System.out.println("Listing objects");
            ObjectListing objectListing = s3.listObjects(new ListObjectsRequest().withBucketName(bucketName)
                    .withPrefix("logshed/logshed_app_id=denali_usage_logs/log_year=2018/log_month=02/log_day=06"));
            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                System.out.println(" - " + objectSummary.getKey() + "  " + "(size = " + objectSummary.getSize() + ")");
            }
            System.out.println();
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }

        /*
         * Download an object - When you download an object, you get all of the object's
         * metadata and a stream from which to read the contents. It's important to read
         * the contents of the stream as quickly as possibly since the data is streamed
         * directly from Amazon S3 and your network connection will remain open until
         * you read all the data or close the input stream.
         *
         * GetObjectRequest also supports several other options, including conditional
         * downloading of objects based on modification times, ETags, and selectively
         * downloading a range of an object.
         */
        System.out.println("Downloading an object");
        S3Object object = s3.getObject(new GetObjectRequest(bucketName,
                "logshed/logshed_app_id=denali_usage_logs/log_year=2018/log_month=02/log_day=06/log_hour=22/logshed_denali_usage_logs_2018-02-06-22-50-GMT_ec1s-logshedcollector-10-250000.gz"));
        System.out.println("Content-Type: " + object.getObjectMetadata().getContentType());
        // displayTextInputStream(object.getObjectContent());
        displayGzInputStream(object.getObjectContent());
    }

    /**
     * Displays the contents of the specified input stream as text.
     *
     * @param input
     *            The input stream to display as text.
     *
     * @throws IOException
     */
    private static void displayTextInputStream(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null)
                break;

            System.out.println("    " + line);
        }
        System.out.println();
    }

    private static void displayGzInputStream(InputStream input) throws Exception {
        CompressedFileReader.processGzipInputStream(input);
    }
}