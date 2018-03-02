package com.james.amazon.s3;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonClientException;
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

public class S3LogJsonSchemaValidator {
    public static void main(String[] args) throws Exception {
        System.out.println("BEGIN..." + System.currentTimeMillis());

        String bucketName = "logsheddev";

        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
                    + "Please make sure that your credentials file is at the correct "
                    + "location (~/.aws/credentials), and is in valid format.", e);
        }

        @SuppressWarnings("deprecation")
        AmazonS3 s3 = new AmazonS3Client(credentials);
        s3.setRegion(Region.getRegion(Regions.US_EAST_1));

        List<String> logPaths = getlogPath(s3, Regions.US_EAST_1, bucketName, "logshed/",
                "logshed_app_id=denali_usage_logs", "2018", "02", "06");
        System.out.println("logPaths.size()=" + logPaths.size());
        // for (String logPath : logPaths) {
        // System.out.println(logPath);
        // }
        //
        // JamesUtil.printDivider();
        //
        // List<List<String>> dividedLogPaths = divideLogPathsWithBatchSize(logPaths,
        // 5);
        // System.out.println("dividedLogPaths.size()=" + dividedLogPaths.size());
        // for (List<String> tempLogPaths : dividedLogPaths) {
        // for (String logPath : tempLogPaths) {
        // System.out.println(logPath);
        // }
        // System.out.println("--------");
        // }
        //
        // JamesUtil.printDivider();

        List<String> sliceLogPaths = logPaths.subList(0, 5);
        System.out.println("sliceLogPaths.size()=" + sliceLogPaths.size());
        for (String logPath : sliceLogPaths) {
            System.out.println("...processing file: " + logPath);
            S3Object object = s3.getObject(new GetObjectRequest(bucketName, logPath));
            CompressedFileReader.processGzipInputStream(object.getObjectContent(), logPath.substring(logPath.lastIndexOf("/")+1,logPath.length()) + ".output");
        }

        System.out.println("END..." + System.currentTimeMillis());
    }

    public static List<String> getlogPath(AmazonS3 s3, Regions region, String bucketName, String keyPrefix, String key,
            String year, String month, String day) {
        if (null == region || null == bucketName || bucketName.isEmpty() || null == keyPrefix || keyPrefix.isEmpty()
                || null == key || key.isEmpty() || null == year || year.isEmpty() || null == month || month.isEmpty()
                || null == day || day.isEmpty()) {
            return null;
        }

        List<String> logPaths = new ArrayList<String>();

        s3.setRegion(Region.getRegion(region));

        String prefixPath = keyPrefix + key + "/log_year=" + year + "/log_month=" + month + "/log_day=" + day;
        System.out.println("prefixPath=" + prefixPath);

        ObjectListing objectListing = s3
                .listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix(prefixPath));
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            logPaths.add(objectSummary.getKey());
        }

        return logPaths;
    }

    public static List<List<String>> divideLogPathsWithBatchSize(List<String> logPaths, int batchSize) {
        if (null == logPaths || logPaths.size() == 0 || batchSize < 0) {
            return null;
        }

        List<List<String>> dividedLogPaths = new ArrayList<List<String>>();

        List<String> tempLogPaths = new ArrayList<String>();
        for (String logPath : logPaths) {
            List<String> tLogPaths = new ArrayList<String>();

            tempLogPaths.add(logPath);
            if (tempLogPaths.size() == batchSize) {
                tLogPaths.addAll(tempLogPaths);
                dividedLogPaths.add(tLogPaths);
                tempLogPaths.clear();
            }
        }

        return dividedLogPaths;
    }
    
}
