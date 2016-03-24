package com.researchworx.genomics.gobjectingestion.folderprocessor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.researchworx.genomics.gobjectingestion.objectstorage.ObjectEngine;
import com.researchworx.genomics.gobjectingestion.plugincore.PluginEngine;

public class OutPathPreProcessor implements Runnable {
	//private String transferStubFile;
	private String transfer_watch_file;
	private String transfer_status_file;
	private String incoming_directory;
    private String bucket_name;
    
	public OutPathPreProcessor() {
		//transferStubFile = PluginEngine.config.getParam("transferfile");
		transfer_watch_file = PluginEngine.config.getParam("pathstage2","transfer_watch_file");
		transfer_status_file = PluginEngine.config.getParam("pathstage2","transfer_status_file");
		incoming_directory = PluginEngine.config.getParam("pathstage2","incoming_directory");
		
		bucket_name = PluginEngine.config.getParam("pathstage2","bucket");
	}

	@Override
	public void run() {
		try {
			PluginEngine.PathProcessorActive = true;
			ObjectEngine oe = new ObjectEngine("pathstage2");
			
			while (PluginEngine.PathProcessorActive)  {
				try {
					//oe.deleteBucketContents(bucket_name);
					List<String> remoteDirs = oe.listBucketDirs(bucket_name);
					List<String> localDirs = getWalkPath(incoming_directory);
					List<String> newDirs = new ArrayList<>();
					
					for(String remoteDir : remoteDirs) {
						if(!localDirs.contains(remoteDir)) {
							if(oe.doesObjectExist(bucket_name, remoteDir + transfer_watch_file)) {
								newDirs.add(remoteDir);
							}
						}
					}
					if(!newDirs.isEmpty()) {
						processBucket(newDirs);
					}
					
					Thread.sleep(30000);
				} catch(Exception ex) {
					System.out.println("PathProcessorActive Error: " + ex.toString());
				}
			}
		} catch(Exception ex) {
			System.out.println("PathProcessor Error: " + ex.toString());
		}
	}
	
	private void processBucket(List<String> newDir) {
		ObjectEngine oe = new ObjectEngine("pathstage2");
		
		for(String remoteDir : newDir) {
			
			System.out.println("MOVE DIRECTORY ! " + remoteDir);
			oe.downloadDirectory(bucket_name, remoteDir, incoming_directory);
			
			List<String> filterList = new ArrayList<String>();
			filterList.add(transfer_status_file);
			String inDir = incoming_directory;
			if(!inDir.endsWith("/")) {
				inDir = inDir + "/";
			}
			inDir = inDir + remoteDir;
			oe = new ObjectEngine("pathstage2");
			if(oe.isSyncDir(bucket_name, remoteDir, inDir,filterList)) {
				System.out.println("Directory Sycned inDir=" + inDir);
				Map<String,String> md5map = oe.getDirMD5(inDir,filterList);
				System.out.println("Set MD5 hash");
				setTransferFileMD5(inDir + transfer_status_file, md5map);
				
			}
		}
	}

	private void processDir(Path dir) {
		String inDir = dir.toString();
		inDir = inDir.substring(0, inDir.length() - transfer_status_file.length() - 1);
		
		String outDir = inDir;
		outDir = outDir.substring(outDir.lastIndexOf("/") + 1,outDir.length());
		
		System.out.println("Start process directory: " + outDir);
		
		ObjectEngine oe = new ObjectEngine("pathstage2");
		String status = transferStatus(dir,"transfer_complete_status");
		List<String> filterList = new ArrayList<String>();
		filterList.add(transfer_status_file);
		
		if(status.equals("no")) {
			Map<String,String> md5map = oe.getDirMD5(inDir,filterList);
			setTransferFileMD5(inDir, md5map);
			if(oe.uploadDirectory(bucket_name, inDir, outDir)) {
				if(setTransferFile(dir)) {
					System.out.println("Directory Transfered inDir=" + inDir + " outDir=" + outDir);
				} else {
					System.out.println("Directory Transfer Failed inDir=" + inDir + " outDir=" + outDir);
				}
			}
		} else if(status.equals("yes")) {
			if(oe.isSyncDir(bucket_name, outDir, inDir,filterList)) {
				System.out.println("Directory Sycned inDir=" + inDir + " outDir=" + outDir);
			}
		}
	}
	
	private boolean setTransferFile(Path dir) {
		boolean isSet = false;
		
		try {
			//transfer_request_flag.txt
			if(dir.toString().toLowerCase().endsWith(transfer_status_file.toLowerCase())) {
				//BufferedReader br = null;
				//BufferedWriter bw = null;
				//File xferFile = null;
				List<String> slist = new ArrayList<>();
				try (BufferedReader br = new BufferedReader(new FileReader(dir.toString()))) {
					//slist = new ArrayList<String>();
					//br = new BufferedReader(new FileReader(dir.toString()));
					//StringBuilder sb = new StringBuilder();
					String line = br.readLine();

					while (line != null) {
						//sb.append(line);
						//sb.append(System.lineSeparator());
						if(line.contains("=")) {
							String[] sline = line.split("=");
							if(sline[0].toLowerCase().equals("transfer_complete_status")) {
								 slist.add("TRANSFER_COMPLETE_STATUS=YES");
							} else {
								slist.add(line);
							}
						}
						line = br.readLine();
					}
					//String everything = sb.toString();
				}/* finally {
					br.close();
				}*/
				try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(dir.toString())))) {
					//xferFile = new File(dir.toString());
					//bw = new BufferedWriter(new FileWriter(xferFile));
					for(String line : slist) {
						bw.write(line + "\n");
					}
				}/* finally {
					bw.close();
				}*/
				String status = transferStatus(dir,"transfer_complete_status");
				if(status.equals("yes")) {
					isSet = true;
				}
			}
    	} catch(Exception ex) {
    		System.out.println("PathProcessor Error : setTransferFile : " + ex.toString());
    	}
		return isSet;
	}
	
	private boolean setTransferFileMD5(String dir, Map<String,String> md5map) {
		boolean isSet = false;
		
		try {
			//transfer_request_flag.txt
			PrintWriter out = null;
    		try {
    			out = new PrintWriter(new BufferedWriter(new FileWriter(dir, true)));
        		
    			for (Map.Entry<String, String> entry : md5map.entrySet()) {
    				String md5file = entry.getKey().replace(incoming_directory, "");
    				if(md5file.startsWith("/")) {
    					md5file = md5file.substring(1);
    				}
    				out.write(md5file + ":" + entry.getValue() + "\n");
    				System.out.println(md5file + ":" + entry.getValue());
    			}
    		}
			finally {
				out.flush();
    		    out.close();
    		}
    	} catch(Exception ex) {
    		System.out.println("PathProcessor Error : setTransferFile : " + ex.toString());
    	}
		return isSet;
	}
	
	private String transferStatus(Path dir, String statusString) {
		String status = "no";
    	try {
			if(dir.toString().toLowerCase().endsWith(transfer_watch_file.toLowerCase())) {
				String tmpPath = dir.toString().replace(transfer_watch_file, transfer_status_file);
				File f = new File(tmpPath);
				if(!f.exists()) {
					if(createTransferFile(dir)) {
						System.out.println("Created new transferfile: " + tmpPath);
					}
				}
			} else if(dir.toString().toLowerCase().endsWith(transfer_status_file.toLowerCase())) {
				//BufferedReader br = new BufferedReader(new FileReader(dir.toString()));
				try (BufferedReader br = new BufferedReader(new FileReader(dir.toString()))) {
					//StringBuilder sb = new StringBuilder();
					String line = br.readLine();

					while (line != null) {
						//sb.append(line);
						//sb.append(System.lineSeparator());
						if(line.contains("=")) {
							String[] sline = line.split("=");
							if(sline[0].toLowerCase().equals(statusString)) {
								if(sline[1].toLowerCase().equals("yes")) {
									status = "yes";
									System.out.println("Status : " + statusString + "=" + status);
								}
							}
						}
						line = br.readLine();
					}
					//String everything = sb.toString();
				}/* finally {
					br.close();
				}*/
			}
    	} catch(Exception ex) {
    		System.out.println("InPathPreProcessor Error : transferStatus : " + ex.toString());
    	}
    	return status;
    }

	private boolean createTransferFile(Path dir) {
		boolean isTransfer = false;
		try {
			/*
			boolean isLock = false;
			while(!isLock)
			{
				FileInputStream in = new FileInputStream(dir.toFile());
				try
				{
					java.nio.channels.FileLock lock = in.getChannel().lock();
					try
					{
						//don't so anything, just check for exclusive lock
						isLock = true;
					}
					finally
					{
						lock.release();
					}
				}
				finally
				{
					in.close();
				}
			}
			*/

			//create file
			String tmpPath = dir.toString().replace(transfer_watch_file, transfer_status_file);
			List<String> lines = Arrays.asList("TRANSFER_READY_STATUS=YES","TRANSFER_COMPLETE_STATUS=NO");
			Path file = Paths.get(tmpPath);
			Files.write(file, lines, Charset.forName("UTF-8"));
    	} catch(Exception ex) {
    		System.out.println("PathPreProcessor createTransferFile Error : " + ex.toString());
    	}
    	return isTransfer;
    }
	
	private List<String> getWalkPath( String path ) {
		if(!path.endsWith("/")) {
			path = path + "/";
		}
    	List<String> dirList = new ArrayList<>();
    	
        File root = new File( path );
        File[] list = root.listFiles();

		if (list == null) { return dirList; }

        for ( File f : list )  {
			if ( f.isDirectory() ) {
                //walkPath( f.getAbsolutePath() );
        		String dir = f.getAbsolutePath().replace(path, "");
        		dirList.add(dir + "/");
            }
        }
        return dirList;
    }
}



