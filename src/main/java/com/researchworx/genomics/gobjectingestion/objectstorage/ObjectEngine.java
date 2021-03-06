package com.researchworx.genomics.gobjectingestion.objectstorage;

//import java.io.ByteArrayInputStream;
import java.io.File;
//import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.util.StringUtils;

import com.researchworx.genomics.gobjectingestion.plugincore.PluginEngine;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
//import com.amazonaws.services.s3.model.ObjectMetadata;
//import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectEngine {
    private static final Logger logger = LoggerFactory.getLogger(ObjectEngine.class);

    private static AmazonS3 conn;
    //private final static String FOLDER_SUFFIX = "/";
    private MD5Tools md5t;
    private int partSize;

    public ObjectEngine(String group) {
        logger.trace("ObjectEngine instantiated [group = {}]", group);
        String accessKey = PluginEngine.config.getParam(group, "accesskey");
        logger.debug("\"accesskey\" from config [{}]", accessKey);
        String secretKey = PluginEngine.config.getParam(group, "secretkey");
        logger.debug("\"secretkey\" from config [{}]", secretKey);
        String endpoint = PluginEngine.config.getParam(group, "endpoint");
        logger.debug("\"endpoint\" from config [{}]", endpoint);
        this.partSize = Integer.parseInt(PluginEngine.config.getParam(group, "uploadpartsizemb"));
        logger.debug("\"uploadpartsizemb\" from config [{}]", this.partSize);

        //String accessKey = PluginEngine.config.getParam("s3","accesskey");
        //String secretKey = PluginEngine.config.getParam("s3","secretkey");
        //String endpoint = PluginEngine.config.getParam("s3","endpoint");
        logger.trace("Building AWS Credentials");
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

        logger.trace("Building ClientConfiguration");
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setProtocol(Protocol.HTTPS);
        clientConfig.setSignerOverride("S3SignerType");

        logger.trace("Connecting to Amazon S3");
        conn = new AmazonS3Client(credentials, clientConfig);
        conn.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
        conn.setEndpoint(endpoint);

        logger.trace("Building new MD5Tools");
        md5t = new MD5Tools(group);

    }

    public boolean uploadDirectory(String bucket, String inDir, String outDir) {
        logger.debug("Call to uploadDirectory [bucket = {}, inDir = {}, outDir = {}]", bucket, inDir, outDir);
        boolean wasTransfered = false;
        TransferManager tx = null;

        try {
            logger.trace("Building new TransferManager");
            tx = new TransferManager(conn);

            logger.trace("Building new TransferManagerConfiguration");
            TransferManagerConfiguration tmConfig = new TransferManagerConfiguration();

            logger.trace("Setting up minimum part size");
            // Sets the minimum part size for upload parts.
            tmConfig.setMinimumUploadPartSize(partSize * 1024 * 1024);
            logger.trace("Setting up size threshold for multipart uploads");
            // Sets the size threshold in bytes for when to use multipart uploads.
            tmConfig.setMultipartUploadThreshold((long) partSize * 1024 * 1024);
            logger.trace("Setting configuration on TransferManager");
            tx.setConfiguration(tmConfig);

            logger.trace("[uploadDir] set to [inDir]");
            File uploadDir = new File(inDir);

            logger.trace("Starting timer");
            long startUpload = System.currentTimeMillis();

            logger.info("Beginning upload [bucket = {}, outDir = {}, uploadDir = {}]", bucket, outDir, uploadDir);
            MultipleFileUpload myUpload = tx.uploadDirectory(bucket, outDir, uploadDir, true);

            // You can poll your transfer's status to check its progress
            while (!myUpload.isDone()) {
                /*
                   System.out.println("Transfer: " + myUpload.getDescription());
				   System.out.println("  - State: " + myUpload.getState());
				   System.out.println("  - Progress: "
								   + myUpload.getProgress().getBytesTransferred());
				*/
                Thread.sleep(1000);
            }

            logger.trace("Calculating upload statistics");
            float transferTime = (System.currentTimeMillis() - startUpload) / 1000;
            long bytesTransfered = myUpload.getProgress().getBytesTransferred();
            float transferRate = (bytesTransfered / 1000000) / transferTime;

            logger.debug("Upload Transfer Desc: " + myUpload.getDescription());
            logger.debug("\t- Transfered : " + myUpload.getProgress().getBytesTransferred() + " bytes");
            logger.debug("\t- Elapsed time : " + transferTime + " seconds");
            logger.debug("\t- Transfer rate : " + transferRate + " MB/sec");

			/*
			   System.out.println("Transfer: " + myUpload.getDescription());
			   System.out.println("  - State: " + myUpload.getState());
			   System.out.println("  - Progress: "
							   + myUpload.getProgress().getBytesTransferred());
			*/
            // Transfers also allow you to set a <code>ProgressListener</code> to receive
            // asynchronous notifications about your transfer's progress.
            //myUpload.addProgressListener(myProgressListener);

            // Or you can block the current thread and wait for your transfer to
            // to complete. If the transfer fails, this method will throw an
            // AmazonClientException or AmazonServiceException detailing the reason.
            //myUpload.waitForCompletion();

            // After the upload is complete, call shutdownNow to release the resources.


            wasTransfered = true;
        } catch (Exception ex) {
            logger.error("uploadDirectory {}", ex.getMessage());
        } finally {
            try {
                assert tx != null;
                tx.shutdownNow();
            } catch (AssertionError e) {
                logger.error("uploadDirectory - TransferManager was pre-emptively shutdown");
            }
        }
        return wasTransfered;
    }

    //	downloadDirectory(String bucketName, String keyPrefix, File destinationDirectory)
    public boolean downloadDirectory(String bucketName, String keyPrefix, String destinationDirectory) {
        logger.debug("Call to downloadDirectory [bucketName = {}, keyPrefix = {}, destinationDirectory = {}", bucketName, keyPrefix, destinationDirectory);
        boolean wasTransfered = false;
        TransferManager tx = null;

        try {
            logger.trace("Building new TransferManager");
            tx = new TransferManager(conn);

            logger.trace("Setting [downloadDir] to [desinationDirectory]");
            File downloadDir = new File(destinationDirectory);
            if (!downloadDir.exists()) {
                if (!downloadDir.mkdirs()) {
                    logger.error("Failed to create download directory!");
                    return false;
                }
            }

            logger.trace("Starting download timer");
            long startDownload = System.currentTimeMillis();
            logger.info("Beginning download [bucketName = {}, keyPrefix = {}, downloadDir = {}", bucketName, keyPrefix, downloadDir);
            MultipleFileDownload myDownload = tx.downloadDirectory(bucketName, keyPrefix, downloadDir);

            //logger.info("Downloading: " + bucketName + ":" + keyPrefix + " to " + downloadDir);
			/*
			myDownload.addProgressListener(new ProgressListener() {
				// This method is called periodically as your transfer progresses
				public void progressChanged(ProgressEvent progressEvent) {
					System.out.println(myDownload.getProgress().getPercentTransferred() + "%");
					System.out.println(progressEvent.getEventType());
					System.out.println(progressEvent.getEventCode());
					System.out.println(ProgressEvent.COMPLETED_EVENT_CODE);
					if (progressEvent.getEventCode() == ProgressEvent.COMPLETED_EVENT_CODE) {
						System.out.println("download complete!!!");
					}
				}
			});
			myDownload.waitForCompletion();
			*/


            while (!myDownload.isDone()) {
                Thread.sleep(1000);
            }

            logger.trace("Calculating download statistics");
            float transferTime = (System.currentTimeMillis() - startDownload) / 1000;
            long bytesTransfered = myDownload.getProgress().getBytesTransferred();
            float transferRate = (bytesTransfered / 1000000) / transferTime;

            logger.debug("Download Transfer Desc: " + myDownload.getDescription());
            logger.debug("\t- Transfered : " + myDownload.getProgress().getBytesTransferred() + " bytes");
            logger.debug("\t- Elapsed time : " + transferTime + " seconds");
            logger.debug("\t- Transfer rate : " + transferRate + " MB/sec");


            // Transfers also allow you to set a <code>ProgressListener</code> to receive
            // asynchronous notifications about your transfer's progress.
            //myUpload.addProgressListener(myProgressListener);

            // Or you can block the current thread and wait for your transfer to
            // to complete. If the transfer fails, this method will throw an
            // AmazonClientException or AmazonServiceException detailing the reason.
            //myUpload.waitForCompletion();

            // After the upload is complete, call shutdownNow to release the resources.


            wasTransfered = true;

        } catch (Exception ex) {
            logger.error("downloadDirectory {}", ex.getMessage());
        } finally {
            try {
                assert tx != null;
                tx.shutdownNow();
            } catch (AssertionError e) {
                logger.error("downloadDirectory - TransferManager was pre-emptively shutdown");
            }
        }
        return wasTransfered;
    }

    /*public void createFolder(String bucket, String foldername) {

        // Create metadata for your folder & set content-length to 0
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);

        // Create empty content
        InputStream emptyContent = new ByteArrayInputStream(new byte[0]);

        // Create a PutObjectRequest passing the foldername suffixed by /
        PutObjectRequest putObjectRequest =
                new PutObjectRequest(bucket, foldername + FOLDER_SUFFIX,
                        emptyContent, metadata);

        // Send request to S3 to create folder
        conn.putObject(putObjectRequest);
    }*/

    public boolean isSyncDir(String bucket, String s3Dir, String localDir, List<String> ignoreList) {
        logger.debug("Call to isSyncDir [bucket = {}, s3Dir = {}, localDir = {}, ignoreList = {}", bucket, s3Dir, localDir, ignoreList.toString());
        boolean isSync = true;
        Map<String, String> mdhp = new HashMap<>();

        try {
            if (!s3Dir.endsWith("/")) {
                s3Dir = s3Dir + "/";
            }
            logger.trace("Grabbing [objects] from [bucket] [s3Dir]");
            ObjectListing objects = conn.listObjects(bucket, s3Dir);
            do {
                for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                    if (!mdhp.containsKey(objectSummary.getKey())) {
                        logger.debug("Adding from s3 [{} : {}]", objectSummary.getKey(), objectSummary.getETag());
                        mdhp.put(objectSummary.getKey(), objectSummary.getETag());
                    }
                }
                logger.trace("Grabbing next batch of [objects]");
                objects = conn.listNextBatchOfObjects(objects);
            } while (objects.isTruncated());

            //S3Object object = conn.getObject(new GetObjectRequest(bucketName, key));

            logger.trace("Grabbing list of files from [localDir]");
            File folder = new File(localDir);
            File[] listOfFiles = folder.listFiles();
            for (File file : listOfFiles != null ? listOfFiles : new File[0]) {
                if ((file.isFile()) && (!ignoreList.contains(file.getName()))) {
                    String bucket_key = s3Dir + file.getName();
                    logger.debug("[bucket_key = {}]", bucket_key);
                    String md5hash;
                    if (mdhp.containsKey(bucket_key)) {
                        logger.trace("[mdhp] contains [bucket_key]");
                        String checkhash = mdhp.get(bucket_key);
                        if (checkhash.contains("-")) {
                            logger.trace("Grabbing multipart-checksum for large/multipart file");
                            md5hash = md5t.getMultiCheckSum(file.getAbsolutePath());
                        } else {
                            logger.trace("Grabbing direct checksum for small/non-multipart file");
                            md5hash = md5t.getCheckSum(file.getAbsolutePath());
                        }
                        if (!md5hash.equals(checkhash)) {
                            isSync = false;
                            logger.debug("Invalid Sync [bucket_key = {}, checkhash = {}] should be [md5hash = {}]", bucket_key, checkhash, md5hash);
                        }
                    } else {
                        logger.debug("Missing Key [bucket_key = {}]", bucket_key);
                        isSync = false;
                    }
                }
            }
        } catch (Exception ex) {
            logger.error("isSyncDir {}", ex.getMessage());
            isSync = false;
        }
        return isSync;

    }

    public Map<String, String> getDirMD5(String localDir, List<String> ignoreList) {
        logger.debug("Call to getDirMD5 [localDir = {}, ignoreList = {}]", localDir, ignoreList.toString());
        Map<String, String> mdhp = new HashMap<>();

        try {
            logger.trace("Grabbing [listOfFiles] from [localDir]");
            File folder = new File(localDir);
            File[] listOfFiles = folder.listFiles();
            logger.trace("Iterating [listOfFiles]");
            for (File file : listOfFiles != null ? listOfFiles : new File[0]) {
                if ((file.isFile()) && (!ignoreList.contains(file.getName()))) {
                    logger.trace("Processing [file = {}]", file.toString());
                    if (!ignoreList.contains(file.getName())) {
                        logger.trace("Adding [file] to [mdhp]");
                        mdhp.put(file.getAbsolutePath(), md5t.getCheckSum(file.getAbsolutePath()));
                    }
                }
            }
        } catch (Exception ex) {
            logger.error("isSyncDir {}", ex.getMessage());
        }
        return mdhp;

    }

    public Map<String, String> listBucketContents(String bucket) {
        logger.debug("Call to listBucketContents [bucket = {}]", bucket);
        Map<String, String> fileMap = new HashMap<>();
        try {
            logger.trace("Grabbing [objects] list from [bucket]");
            ObjectListing objects = conn.listObjects(bucket);
            do {
                for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                    logger.debug("Found object {}\t{}\t{}\t{}", objectSummary.getKey(),
                            objectSummary.getSize(),
                            objectSummary.getETag(),
                            StringUtils.fromDate(objectSummary.getLastModified()));
                    fileMap.put(objectSummary.getKey(), objectSummary.getETag());
                }
                logger.trace("Grabbing next batch of [objects]");
                objects = conn.listNextBatchOfObjects(objects);
            } while (objects.isTruncated());
        } catch (Exception ex) {
            logger.error("listBucketContents {}", ex.getMessage());
            fileMap = null;
        }
        return fileMap;
    }

    public List<String> listBucketDirs(String bucket) {
        logger.debug("Call to listBucketDirs [bucket = {}", bucket);
        List<String> dirList = new ArrayList<>();
        try {
            logger.trace("Instantiating new ListObjectsRequest");
            ListObjectsRequest lor = new ListObjectsRequest();
            lor.setBucketName(bucket);
            lor.setDelimiter("/");

            logger.trace("Grabbing [objects] list from [lor]");
            ObjectListing objects = conn.listObjects(lor);
            do {
                List<String> sublist = objects.getCommonPrefixes();
                logger.trace("Adding all Common Prefixes from [objects]");
                dirList.addAll(sublist);
                logger.trace("Grabbing next batch of [objects]");
                objects = conn.listNextBatchOfObjects(objects);
            } while (objects.isTruncated());
        } catch (Exception ex) {
            logger.error("listBucketDirs {}", ex.getMessage());
            dirList = null;
        }
        return dirList;
    }

    public Map<String, String> listBucketContents(String bucket, String searchName) {
        logger.debug("Call to listBucketContents [bucket = {}, searchName = {}]", bucket, searchName);
        Map<String, String> fileMap = new HashMap<>();
        try {
            logger.trace("Grabbing [objects] list from [bucket]");
            ObjectListing objects = conn.listObjects(bucket);
            do {
                for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                    if (objectSummary.getKey().contains(searchName)) {
                        logger.debug("Found object {}\t{}\t{}\t{}", objectSummary.getKey(),
                                objectSummary.getSize(),
                                objectSummary.getETag(),
                                StringUtils.fromDate(objectSummary.getLastModified()));
                        fileMap.put(objectSummary.getKey(), objectSummary.getETag());
                    }
                }
                objects = conn.listNextBatchOfObjects(objects);
            } while (objects.isTruncated());
        } catch (Exception ex) {
            logger.error("listBucketContents {}", ex.getMessage());
            fileMap = null;
        }
        return fileMap;
    }

    public boolean doesObjectExist(String bucket, String objectName) {
        logger.debug("Call to doesObjectExist [bucket = {}, objectName = {}]", bucket, objectName);
        return conn.doesObjectExist(bucket, objectName);
    }

    public boolean doesBucketExist(String bucket) {
        logger.debug("Call to doesBucketExist [bucket = {}]", bucket);
        return conn.doesBucketExist(bucket);
    }

    public void createBucket(String bucket) {
        logger.debug("Call to createBucket [bucket = {}]", bucket);
        try {
            if (!conn.doesBucketExist(bucket)) {
                Bucket mybucket = conn.createBucket(bucket);
                logger.debug("Created bucket [{}] on [{}]", bucket, mybucket.getCreationDate().toString());
            }
        } catch (Exception ex) {
            logger.error("createBucket {}", ex.toString());
        }

    }

    public void deleteBucketContents(String bucket) {
        logger.debug("Call to deleteBucketContents [bucket = {}]", bucket);
        logger.trace("Grabbing [objects] list from [bucket]");
        ObjectListing objects = conn.listObjects(bucket);
        do {
            for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                logger.trace("Deleting [{}] object from [{}] bucket", objectSummary.getKey(), bucket);
                conn.deleteObject(bucket, objectSummary.getKey());

                logger.debug("Deleted {}\t{}\t{}\t{}", objectSummary.getKey(),
                        objectSummary.getSize(),
                        objectSummary.getETag(),
                        StringUtils.fromDate(objectSummary.getLastModified()));
            }
            objects = conn.listNextBatchOfObjects(objects);
        } while (objects.isTruncated());
    }
}
