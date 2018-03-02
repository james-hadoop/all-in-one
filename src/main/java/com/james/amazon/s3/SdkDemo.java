package com.james.amazon.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

public class SdkDemo {
    public static void main(String[] args) throws Exception {
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException("Cannot load the credentials from the credential profiles file. ", e);
        }

        AmazonS3 s3 = new AmazonS3Client(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
       
        s3.setRegion(usEast1);
        String bucketName = "tndpp";
        String key = "s3FilePath";
        boolean isFileExist = s3.doesObjectExist(bucketName, key);
        System.out.println("isFileExist=" + isFileExist);
        
        s3.setRegion(usEast1);
        bucketName = "logsheddev";
        key = "s3FilePath";
        isFileExist = s3.doesObjectExist(bucketName, key);
        System.out.println("isFileExist=" + isFileExist);
        
        s3.setRegion(usEast1);
        bucketName = "s3FilePath";
        isFileExist = s3.doesBucketExist(bucketName);
        System.out.println("isFileExist=" + isFileExist);
    }
}