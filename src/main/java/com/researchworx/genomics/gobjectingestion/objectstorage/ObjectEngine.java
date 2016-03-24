package com.researchworx.genomics.gobjectingestion.objectstorage;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
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
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;

public class ObjectEngine {

	private static AmazonS3 conn;
	private final static String FOLDER_SUFFIX = "/";
	private MD5Tools md5t;
	private int partSize;
	
	public ObjectEngine(String group) {
		String accessKey = PluginEngine.config.getParam(group,"accesskey");
		String secretKey = PluginEngine.config.getParam(group,"secretkey");
		String endpoint = PluginEngine.config.getParam(group,"endpoint");
		this.partSize = Integer.parseInt(PluginEngine.config.getParam(group,"uploadpartsizemb"));
		
		//String accessKey = PluginEngine.config.getParam("s3","accesskey");
		//String secretKey = PluginEngine.config.getParam("s3","secretkey");
		//String endpoint = PluginEngine.config.getParam("s3","endpoint");
		AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
		
		ClientConfiguration clientConfig = new ClientConfiguration();
		clientConfig.setProtocol(Protocol.HTTPS);
		clientConfig.setSignerOverride("S3SignerType");
		
		conn = new AmazonS3Client(credentials, clientConfig);
		conn.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
		conn.setEndpoint(endpoint);
		
		md5t = new MD5Tools(group);
	}
	
	public boolean uploadDirectory(String bucket, String inDir, String outDir) {
		boolean wasTransfered = false;
		TransferManager tx = null;
		
		try {
			tx = new TransferManager(conn);

			TransferManagerConfiguration tmConfig = new TransferManagerConfiguration();

			// Sets the minimum part size for upload parts.
			tmConfig.setMinimumUploadPartSize(partSize * 1024 * 1024);
			// Sets the size threshold in bytes for when to use multipart uploads.
			tmConfig.setMultipartUploadThreshold((long)partSize * 1024 * 1024);
			tx.setConfiguration(tmConfig);

			File uploadDir = new File(inDir);

			long startUpload = System.currentTimeMillis();

			MultipleFileUpload myUpload = tx.uploadDirectory(bucket, outDir, uploadDir, true);

			// You can poll your transfer's status to check its progress
			while (myUpload.isDone() == false) {
				/*
				   System.out.println("Transfer: " + myUpload.getDescription());
				   System.out.println("  - State: " + myUpload.getState());
				   System.out.println("  - Progress: "
								   + myUpload.getProgress().getBytesTransferred());
				*/
				Thread.sleep(1000);
			}

			float transferTime = (System.currentTimeMillis() - startUpload) / 1000 ;
			long bytesTransfered = myUpload.getProgress().getBytesTransferred();
			float transferRate = (bytesTransfered/1000000)/transferTime;

			System.out.println("Transfer Desc: " + myUpload.getDescription());
			System.out.println("  - Transfered : " + myUpload.getProgress().getBytesTransferred() + " bytes");
			System.out.println("  - Elapsed time : " + transferTime + " seconds");
			System.out.println("  - Transfer rate : " + transferRate + " MB/sec");

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
		} catch(Exception ex) {
			System.out.println("ObjectEngine : UploadDirectory Error : " + ex.toString());
		} finally {
			tx.shutdownNow();
		}
		return wasTransfered;
	}
	
	//	downloadDirectory(String bucketName, String keyPrefix, File destinationDirectory)
	public boolean downloadDirectory(String bucketName, String keyPrefix, String destinationDirectory) {
		boolean wasTransfered = false;
		TransferManager tx = null;
		
		try {
			tx = new TransferManager(conn);

			File downloadDir = new File(destinationDirectory);
			if(!downloadDir.exists()) {
				downloadDir.mkdirs();
			}

			long startDownload = System.currentTimeMillis();
			MultipleFileDownload myDownload = tx.downloadDirectory(bucketName, keyPrefix, downloadDir);

			System.out.println("Downloading: " + bucketName + ":" + keyPrefix + " to " + downloadDir);
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
			float transferTime = (System.currentTimeMillis() - startDownload) / 1000 ;
			long bytesTransfered = myDownload.getProgress().getBytesTransferred();
			float transferRate = (bytesTransfered/1000000)/transferTime;

			System.out.println("Transfer Desc: " + myDownload.getDescription());
			System.out.println("  - Transfered : " + myDownload.getProgress().getBytesTransferred() + " bytes");
			System.out.println("  - Elapsed time : " + transferTime + " seconds");
			System.out.println("  - Transfer rate : " + transferRate + " MB/sec");



			// Transfers also allow you to set a <code>ProgressListener</code> to receive
			// asynchronous notifications about your transfer's progress.
			//myUpload.addProgressListener(myProgressListener);

			// Or you can block the current thread and wait for your transfer to
			// to complete. If the transfer fails, this method will throw an
			// AmazonClientException or AmazonServiceException detailing the reason.
			//myUpload.waitForCompletion();

			// After the upload is complete, call shutdownNow to release the resources.
			
			
			wasTransfered = true;
			
		} catch(Exception ex) {
			System.out.println("ObjectEngine : UploadDirectory Error : " + ex.toString());
		} finally {
			tx.shutdownNow();
		}
		return wasTransfered;
	}
	
	public void createFolder(String bucket, String foldername) {

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
	}
	
	public boolean isSyncDir(String bucket, String s3Dir, String localDir, List<String> ignoreList) {
		boolean isSync = true;
		Map<String,String> mdhp = new HashMap<>();
		
		try {
			if(!s3Dir.endsWith("/")) {
				s3Dir = s3Dir + "/";
			}
			ObjectListing objects = conn.listObjects(bucket, s3Dir);
			do {
				for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
					if(!mdhp.containsKey(objectSummary.getKey())) {
						//System.out.println("adding from s3 " + objectSummary.getKey() + " " + objectSummary.getETag());
						mdhp.put(objectSummary.getKey(), objectSummary.getETag());
					}
				}
				objects = conn.listNextBatchOfObjects(objects);
			} while (objects.isTruncated()); 
			
			//S3Object object = conn.getObject(new GetObjectRequest(bucketName, key));
			
			
			File folder = new File(localDir);
			File[] listOfFiles = folder.listFiles();
			for (int i = 0; i < listOfFiles.length; i++) {
				if ((listOfFiles[i].isFile()) && (!ignoreList.contains(listOfFiles[i].getName()))) {
					String bucket_key = s3Dir + listOfFiles[i].getName();
					String md5hash = null;
					if(mdhp.containsKey(bucket_key)) {
						String checkhash = mdhp.get(bucket_key).toString();
						if(checkhash.contains("-")) {
							//large file or part of multipart, use multipart checksum
							md5hash = md5t.getMultiCheckSum(listOfFiles[i].getAbsolutePath());
						} else {
							//small file or not multipart, use direct checksum
							md5hash = md5t.getCheckSum(listOfFiles[i].getAbsolutePath());
						}
						if(!md5hash.equals(checkhash)) {
							isSync = false;
							System.out.println("false synced: " + bucket_key + " " + checkhash + " should be " + md5hash);
						}
					} else {
						System.out.println("Missing synced: " + bucket_key + " " + md5hash);
						isSync = false;
					}
				}
			}
		} catch(Exception ex) {
			System.out.println("ObjectEngine : isSyncDir " + ex.toString());
			isSync = false;
		}
		return isSync;
		
	}

	public Map<String,String> getDirMD5(String localDir, List<String> ignoreList) {
		Map<String,String> mdhp = new HashMap<>();
		
		try {
			File folder = new File(localDir);
			File[] listOfFiles = folder.listFiles();
			
			for (int i = 0; i < listOfFiles.length; i++) {
				if ((listOfFiles[i].isFile()) && (!ignoreList.contains(listOfFiles[i].getName()))) {
					if(!ignoreList.contains(listOfFiles[i].getName())) {
						mdhp.put(listOfFiles[i].getAbsolutePath(), md5t.getCheckSum(listOfFiles[i].getAbsolutePath()));
					}
				}
			}
		} catch(Exception ex) {
			System.out.println("ObjectEngine : isSyncDir " + ex.toString());
		}
		return mdhp;
		
	}

	public Map<String,String> listBucketContents(String bucket) {
		Map<String,String> fileMap = null;
		try {
			fileMap = new HashMap<String,String>();
			ObjectListing objects = conn.listObjects(bucket);
			do {
		        for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
		                //System.out.println(objectSummary.getKey() + "\t" +
		                //        objectSummary.getSize() + "\t" +
		                //        objectSummary.getETag() + "\t" +
		                //        StringUtils.fromDate(objectSummary.getLastModified()));
		        	fileMap.put(objectSummary.getKey(),objectSummary.getETag());
		        }
		        objects = conn.listNextBatchOfObjects(objects);
			} while (objects.isTruncated());
		} catch(Exception ex) {
			System.out.println("ObjectEngine : listbucket Error " + ex.toString());
			fileMap = null;
		}
		return fileMap;
	}
	
	public List<String> listBucketDirs(String bucket) {
		List<String> dirList = new ArrayList<>();
		try {
			ListObjectsRequest lor = new ListObjectsRequest();
			lor.setBucketName(bucket);
			lor.setDelimiter("/");
			
			ObjectListing objects = conn.listObjects(lor);
			do {
				List<String> sublist = objects.getCommonPrefixes();
				dirList.addAll(sublist);
		        objects = conn.listNextBatchOfObjects(objects);
			} while (objects.isTruncated());
		} catch(Exception ex) {
			System.out.println("ObjectEngine : listbucket Error " + ex.toString());
			dirList = null;
		}
		return dirList;
	}
	
	public Map<String,String> listBucketContents(String bucket, String searchName) {
		Map<String,String> fileMap = new HashMap<>();
		try {
			ObjectListing objects = conn.listObjects(bucket);
			do {
		        for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
		                //System.out.println(objectSummary.getKey() + "\t" +
		                //        objectSummary.getSize() + "\t" +
		                //        objectSummary.getETag() + "\t" +
		                //        StringUtils.fromDate(objectSummary.getLastModified()));
		        	if(objectSummary.getKey().contains(searchName)) {
		        		fileMap.put(objectSummary.getKey(),objectSummary.getETag());
		        	}
		        }
		        objects = conn.listNextBatchOfObjects(objects);
			} while (objects.isTruncated());
		} catch(Exception ex) {
			System.out.println("ObjectEngine : listbucket Error " + ex.toString());
			fileMap = null;
		}
		return fileMap;
	}
	
	public boolean doesObjectExist(String bucket, String objectName) {
		//doesObjectExist
		return conn.doesObjectExist(bucket, objectName);
		//return true;
	}

	public boolean doesBucketExist(String bucket)
	{
		return conn.doesBucketExist(bucket);
	}
	
	public void createBucket(String bucket) {
		try {
			if(!conn.doesBucketExist(bucket)) {
				Bucket mybucket = conn.createBucket(bucket);
				System.out.println("Created bucket : " + bucket + " " + mybucket.getCreationDate().toString());
			}
		} catch(Exception ex) {
			System.out.println("ObjectEngine : createBucket Error " + ex.toString());
		}
		
	}
	
	public void deleteBucketContents(String bucket) {
		ObjectListing objects = conn.listObjects(bucket);
		do {
			for (S3ObjectSummary objectSummary : objects.getObjectSummaries())
			{
				conn.deleteObject(bucket, objectSummary.getKey() );

				System.out.println("Deleted " + objectSummary.getKey() + "\t" +
					objectSummary.getSize() + "\t" +
					objectSummary.getETag() + "\t" +
					StringUtils.fromDate(objectSummary.getLastModified()));
			}
			objects = conn.listNextBatchOfObjects(objects);
		} while (objects.isTruncated());
	}
}
