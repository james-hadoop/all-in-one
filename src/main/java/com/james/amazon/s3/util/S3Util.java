package com.james.amazon.s3.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;

public class S3Util {
    private static Logger logger = Logger.getLogger(S3Util.class);

    /**
     * check if s3 path exist. Use list sub path object to check.
     * 
     * @param s3Path,
     *            lie
     *            s3://logsheddev/logshed/logshed_app_id=denali_usage_logs/log_year=2017/log_month=06/log_day=07/
     * @return
     */
    public static boolean isS3PathExist(String s3Path) {
        boolean isExisted = false;

        if (null == s3Path || 0 == s3Path.length()) {
            isExisted = false;
        }

        try {
            AmazonS3 s3 = new AmazonS3Client();
            int s3PrefixLength = "s3://".length();
            int firstDelimiter = s3Path.indexOf("/", s3PrefixLength);
            if (firstDelimiter > 0) {
                String bucketName = s3Path.substring(s3PrefixLength, firstDelimiter);
                String objectKey = s3Path.substring(firstDelimiter + 1);
                ListObjectsRequest request = new ListObjectsRequest();
                request.setBucketName(bucketName);
                request.setPrefix(objectKey);
                request.setMaxKeys(1);
                ObjectListing list = s3.listObjects(request);
                if (list.getObjectSummaries().size() == 1) {
                    isExisted = true;
                }
            }
        } catch (Exception e) {
            logger.error("isS3PathExist error:s3Path=" + s3Path, e);
        }

        return isExisted;
    }

    public static boolean doesBucketExist(String s3Path, AWSCredentials credentials) {
        if (null == credentials) {
            return false;
        }

        boolean isExisted = false;

        if (null == s3Path || 0 == s3Path.length()) {
            isExisted = false;
        }

        String bucketName = s3Path;

        try {
            AmazonS3 s3 = new AmazonS3Client(credentials);
            Region usEast1 = Region.getRegion(Regions.US_EAST_1);

            if (s3Path.contains("s3://")) {
                bucketName = bucketName.replace("s3://", "");
            }

            s3.setRegion(usEast1);

            isExisted = s3.doesBucketExist(bucketName);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return isExisted;
    }

    public static boolean doesBucketExist(String s3Path) {
        boolean isExisted = false;

        if (null == s3Path || 0 == s3Path.length()) {
            isExisted = false;
        }

        String bucketName = s3Path;

        try {
            AmazonS3 s3 = new AmazonS3Client();
            Region usEast1 = Region.getRegion(Regions.US_EAST_1);

            if (s3Path.contains("s3://")) {
                bucketName = bucketName.replace("s3://", "");
            }

            s3.setRegion(usEast1);

            isExisted = s3.doesBucketExist(bucketName);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return isExisted;
    }

    public static List<String> removeNoExsitedS3Paths(String[] s3Paths) {
        if (null == s3Paths || 0 == s3Paths.length) {
            return null;
        }

        List<String> exsitedS3Paths = new ArrayList<String>();
        for (String path : s3Paths) {
            if (S3Util.doesBucketExist(path)) {
                exsitedS3Paths.add(path);
            }
        }

        return exsitedS3Paths;
    }
}