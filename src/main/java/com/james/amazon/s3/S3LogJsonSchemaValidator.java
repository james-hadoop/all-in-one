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
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3LogJsonSchemaValidator {
    public static void main(String[] args) throws Exception {
        List<String> logPaths = getlogPath(Regions.US_EAST_1, "logsheddev", "logshed/",
                "logshed_app_id=denali_usage_logs", "2018", "02", "06");
        System.out.println("logPaths.size()=" + logPaths.size());
        for (String logPath : logPaths) {
            System.out.println(logPath);
        }

        System.out.println("---------------------------- divider ----------------------------");

        List<List<String>> dividedLogPaths = divideLogPathsWithBatchSize(logPaths, 5);
        System.out.println("dividedLogPaths.size()=" + dividedLogPaths.size());
        for (List<String> tempLogPaths : dividedLogPaths) {
            for (String logPath : tempLogPaths) {
                System.out.println(logPath);
            }
            System.out.println("--------");
        }
        
        System.out.println("---------------------------- divider ----------------------------");
        
        
    }

    public static List<String> getlogPath(Regions region, String bucketName, String keyPrefix, String key, String year,
            String month, String day) {
        if (null == region || null == bucketName || bucketName.isEmpty() || null == keyPrefix || keyPrefix.isEmpty()
                || null == key || key.isEmpty() || null == year || year.isEmpty() || null == month || month.isEmpty()
                || null == day || day.isEmpty()) {
            return null;
        }

        List<String> logPaths = new ArrayList<String>();

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
