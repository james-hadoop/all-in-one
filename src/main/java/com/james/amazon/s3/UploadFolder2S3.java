package com.james.amazon.s3;

import java.io.File;
import java.util.Date;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.james.common.util.DateUtil;

public class UploadFolder2S3 {

    public static void main(String[] args) {
        if (null == args || 11 != args.length) {
            System.err.println("arguments count should be 8\n");
            System.err.println(
                    "Usage:\n\tjava -jar UploadFolder2S3.jar regionName bucketName keyPrefix key year month day threadCount batchSize filterFlag");
            System.err.println(
                    "Example:\n\tjava -jar UploadFolder2S3.jar US_EAST_1 logsheddev logshed logshed_app_id=denali_usage_logs 2018 04 13 10 10 false");
        }

        String regionName = args[0];
        String bucketName = args[1];
        String keyPrefix = args[2];
        String key = args[3];
        String year = args[4];
        String month = args[5];
        String day = args[6];
        int threadCount = Integer.parseInt(args[7]);
        int batchSize = Integer.parseInt(args[8]);

        boolean flagFilter = false;
        if (args[9].equals("true")) {
            flagFilter = true;
        }

        String folderPath = args[10];

        Regions region = Regions.US_EAST_1;
        if ("us-west-2".equalsIgnoreCase(regionName)) {
            region = Regions.US_WEST_2;
        }

        System.out.println("regionName=" + regionName);
        System.out.println("bucketName=" + bucketName);
        System.out.println("keyPrefix=" + keyPrefix);
        System.out.println("key=" + key);
        System.out.println("year=" + year);
        System.out.println("month=" + month);
        System.out.println("day=" + day);
        System.out.println("threadCount=" + threadCount);
        System.out.println("batchSize=" + batchSize);
        System.out.println("filterFlag=" + flagFilter);
        System.out.println("folderPath=" + folderPath);

        if (null == folderPath || folderPath.isEmpty()) {
            return;
        }

        System.out.println("BEGIN..." + DateUtil.DateToString(new Date(), DateUtil.YYYY_MM_DD_HH_MM_SS));

        @SuppressWarnings("deprecation")
        AmazonS3 s3 = new AmazonS3Client();
        //s3.setRegion(Region.getRegion(region));
        
        s3.putObject(new PutObjectRequest(bucketName, key, new File(folderPath)));
    }
}
