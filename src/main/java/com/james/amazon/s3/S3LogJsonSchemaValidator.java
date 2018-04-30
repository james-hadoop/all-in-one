package com.james.amazon.s3;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

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
import com.james.common.util.DateUtil;
import com.james.common.util.JamesUtil;
import com.james.demo.file.CompressedFileReader;

public class S3LogJsonSchemaValidator {
    public static final String OUTPUT_DIR = "data" + File.separator + "json_validation_output" + File.separator;

    public static void main(String[] args) throws Exception {
        System.out.println("BEGIN..." + DateUtil.DateToString(new Date(), DateUtil.YYYY_MM_DD_HH_MM_SS));

        int threadCount = 20;
        String bucketName = "logsheddev";
        String keyPrefix = "logshed/";
        String key = "logshed_app_id=denali_usage_logs";
        // String year = "2018";
        // String month = "02";
        // String day = "06";
        String year = "2018";
        String month = "03";
        String day = "16";
        int batchSize = 20;

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

        // List<String> logPaths = getlogPath(s3, Regions.US_EAST_1, bucketName,
        // "logshed/",
        // "logshed_app_id=denali_usage_logs", "2018", "02", "06");
        List<String> logPaths = getlogPath(s3, Regions.US_EAST_1, bucketName, keyPrefix, key, year, month, day);
        System.out.println("logPaths.size()=" + logPaths.size());
        // for (String logPath : logPaths) {
        // System.out.println(logPath);
        // }
        //
        JamesUtil.printDivider();

        List<List<String>> dividedLogPaths = divideLogPathsWithBatchSize(logPaths, batchSize);
        System.out.println("dividedLogPaths.size()=" + dividedLogPaths.size());
        for (List<String> tempLogPaths : dividedLogPaths) {
            for (String logPath : tempLogPaths) {
                System.out.println(logPath);
            }
            System.out.println("--------");
        }

        JamesUtil.printDivider();

        // List<String> sliceLogPaths = logPaths.subList(0, 5);
        // System.out.println("sliceLogPaths.size()=" + sliceLogPaths.size());
        // for (String logPath : sliceLogPaths) {
        // System.out.println("...processing file: " + logPath);
        // S3Object object = s3.getObject(new GetObjectRequest(bucketName, logPath));
        // CompressedFileReader.processGzipInputStream(object.getObjectContent(),
        // logPath.substring(logPath.lastIndexOf("/") + 1, logPath.length()) +
        // ".output", false);
        // }
        //
        // JamesUtil.printDivider();

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < dividedLogPaths.size(); i++) {
            JsonFormatValidatorThread thread = new JsonFormatValidatorThread(i, dividedLogPaths.get(i), s3, bucketName);
            // executor.execute(thread);
            executor.submit(thread);
        }
        executor.shutdown();

        System.out.println("END..." + DateUtil.DateToString(new Date(), DateUtil.YYYY_MM_DD_HH_MM_SS));
    }

    /**
     * getlogPath
     * 
     * @param s3
     * @param region
     * @param bucketName
     * @param keyPrefix
     * @param key
     * @param year
     * @param month
     * @param day
     * @return
     */
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

    /**
     * divideLogPathsWithBatchSize
     * 
     * @param logPaths
     * @param batchSize
     * @return
     */
    public static List<List<String>> divideLogPathsWithBatchSize(List<String> logPaths, int batchSize) {
        if (null == logPaths || logPaths.size() == 0 || batchSize < 0) {
            return null;
        }

        List<List<String>> dividedLogPaths = new ArrayList<List<String>>();

        List<String> tempLogPaths = new ArrayList<String>();
        for (String logPath : logPaths) {
            if(!logPath.endsWith(".gz")) {
                continue;
            }
            
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

class JsonFormatValidatorThread extends Thread {
    private int threadId;
    private List<String> sliceLogPaths;
    private AmazonS3 s3;
    private String bucketName;

    public JsonFormatValidatorThread(int threadId, List<String> sliceLogPaths, AmazonS3 s3, String bucketName) {
        this.threadId = threadId;
        this.sliceLogPaths = sliceLogPaths;
        this.s3 = s3;
        this.bucketName = bucketName;
    }

    @Override
    public void run() {
        System.out.println("Thread ID=" + threadId + "  sliceLogPaths.size()=" + sliceLogPaths.size());
        for (String logPath : sliceLogPaths) {
            String processingTime = DateUtil.DateToString(new Date(), DateUtil.YYYYMMDDHHMMSS);
            System.out.println("\tThread ID= " + threadId + " is processing file " + logPath + " at "
                    + DateUtil.DateToString(new Date(), DateUtil.YYYY_MM_DD_HH_MM_SS));
            S3Object object = s3.getObject(new GetObjectRequest(bucketName, logPath));
            try {
                CompressedFileReader.processGzipInputStream(object.getObjectContent(),
                        S3LogJsonSchemaValidator.OUTPUT_DIR
                                + logPath.substring(logPath.lastIndexOf("/") + 1, logPath.length()) + processingTime,
                        false);
            } catch (Exception e) {
                System.err.println("Thread ID=" + threadId + " Exception caught when processing " + logPath);
                e.printStackTrace();
            }
        }
        System.out.println("Thread ID= " + threadId + " finish processing files at "
                + DateUtil.DateToString(new Date(), DateUtil.YYYY_MM_DD_HH_MM_SS));
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}