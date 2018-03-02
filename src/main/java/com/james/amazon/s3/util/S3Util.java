package com.james.amazon.s3.util;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

public class S3Util {
    public static boolean doesBucketExist(String s3Path) {
        boolean isExisted = false;

        if (null == s3Path || 0 == s3Path.length()) {
            isExisted = false;
        }

        String bucketName = s3Path;

        try {
            AWSCredentials credentials = null;
            credentials = new ProfileCredentialsProvider().getCredentials();

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