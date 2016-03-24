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

public class InPathPreProcessor implements Runnable {

	//private String transferStubFile;
	private String transfer_watch_file;
	private String transfer_status_file;
    
    private String bucket_name;
    
	public InPathPreProcessor()
	{
		//transferStubFile = PluginEngine.config.getParam("transferfile");
		transfer_watch_file = PluginEngine.config.getParam("pathstage1","transfer_watch_file");
		transfer_status_file = PluginEngine.config.getParam("pathstage1","transfer_status_file");
		
		bucket_name = PluginEngine.config.getParam("pathstage1","bucket");
	}
		
	public void run() 
	{
		try
		{
			PluginEngine.PathProcessorActive = true;
			ObjectEngine oe = new ObjectEngine("pathstage1");
			oe.createBucket(bucket_name);
			while (PluginEngine.PathProcessorActive) 
			{
				try
				{
					//check if we should process folder
					Path dir = PluginEngine.pathQueue.poll();
					if(dir != null)
					{
						System.out.println("New file: " + dir);
						//if transfer file exist
						String status = transferStatus(dir,"transfer_ready_status");
						if(status != null)
		            	{
							if(status.equals("yes"))
							{
								//process dir
								processDir(dir);	
							}
		            	}
					}
					else
					{
						Thread.sleep(1000);
					}
				}
				catch(Exception ex)
				{
					System.out.println("PathProcessorActive Error: " + ex.toString());
				}
			}
		}
		catch(Exception ex)
		{
			System.out.println("PathProcessor Error: " + ex.toString());
		}
	}
	
	private void processDir(Path dir)
	{
		String inDir = dir.toString();
		inDir = inDir.substring(0, inDir.length() - transfer_status_file.length() - 1);
		
		String outDir = inDir;
		outDir = outDir.substring(outDir.lastIndexOf("/") + 1,outDir.length());
		
		System.out.println("Start process directory: " + outDir);
		
		ObjectEngine oe = new ObjectEngine("pathstage1");
		String status = transferStatus(dir,"transfer_complete_status");
		List<String> filterList = new ArrayList<String>();
		filterList.add(transfer_status_file);
		
		if(status.equals("no"))
		{
			Map<String,String> md5map = oe.getDirMD5(inDir,filterList);
			System.out.println("Set MD5 hash");
			setTransferFileMD5(dir, md5map);
			if(oe.uploadDirectory(bucket_name, inDir, outDir))
			{
				
				if(setTransferFile(dir))
				{
					System.out.println("Directory Transfered inDir=" + inDir + " outDir=" + outDir);
				}
				else
				{
					System.out.println("Directory Transfer Failed inDir=" + inDir + " outDir=" + outDir);
				}
			}
		}
		else if(status.equals("yes"))
		{
			if(oe.isSyncDir(bucket_name, outDir, inDir,filterList))
			{
				System.out.println("Directory Sycned inDir=" + inDir + " outDir=" + outDir);
			}
		}
	}
	
	private boolean setTransferFile(Path dir)
	{
		boolean isSet = false;
		
		try
    	{
    	//transfer_request_flag.txt
    	if(dir.toString().toLowerCase().endsWith(transfer_status_file.toLowerCase()))
    	{
    		BufferedReader br = null;
    		BufferedWriter bw = null;
    		File xferFile = null;
    		List<String> slist = null;
    		try {
    			slist = new ArrayList<String>();
    			br = new BufferedReader(new FileReader(dir.toString()));
    			//StringBuilder sb = new StringBuilder();
    		    String line = br.readLine();
    		    
    		    while (line != null) {
    		        //sb.append(line);
    		        //sb.append(System.lineSeparator());
    		        if(line.contains("="))
    		        {
    		        	String[] sline = line.split("=");
    		        	if(sline[0].toLowerCase().equals("transfer_complete_status"))
    		        	{
    		        		 slist.add("TRANSFER_COMPLETE_STATUS=YES");
    		        	}
    		        	else
    		        	{
    		        		slist.add(line);
    		        	}
    		        	
    		        }
    		    	line = br.readLine();
    		    }
    		    //String everything = sb.toString();
    		} finally {
    		    br.close();
    		}
    		try 
    		{
        		xferFile = new File(dir.toString());
    			bw = new BufferedWriter(new FileWriter(xferFile));
    			for(String line : slist)
    			{
    				bw.write(line + "\n");
    			}
    		}
			finally {
    		    bw.close();
    		}
    		String status = transferStatus(dir,"transfer_complete_status");
    		if(status.equals("yes"))
    		{
    			isSet = true;
    		}
    	}
    	}
    	catch(Exception ex)
    	{
    		System.out.println("PathProcessor Error : setTransferFile : " + ex.toString());
    	}
		
		
		return isSet;
	}
	
	private boolean setTransferFileMD5(Path dir, Map<String,String> md5map)
	{
		boolean isSet = false;
		
		try
    	{
		String watchDirectoryName = PluginEngine.config.getParam("pathstage1","watchdirectory");
    		
    	//transfer_request_flag.txt
    	if(dir.toString().toLowerCase().endsWith(transfer_status_file.toLowerCase()))
    	{
    		PrintWriter out = null;
    		try 
    		{
    			out = new PrintWriter(new BufferedWriter(new FileWriter(dir.toString(), true)));
        		
    			for (Map.Entry<String, String> entry : md5map.entrySet())
    			{
    				String md5file = entry.getKey().replace(watchDirectoryName, "");
    				if(md5file.startsWith("/"))
    				{
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
    	}
    	}
    	catch(Exception ex)
    	{
    		System.out.println("PathProcessor Error : setTransferFile : " + ex.toString());
    	}
		
		
		return isSet;
	}
	
	private String transferStatus(Path dir, String statusString)
    {
		String status = "no";
    	try
    	{
    	if(dir.toString().toLowerCase().endsWith(transfer_watch_file.toLowerCase()))
        {
    		String tmpPath = dir.toString().replace(transfer_watch_file, transfer_status_file);
    		File f = new File(tmpPath);
    		if(!f.exists()) 
    		{ 
    			if(createTransferFile(dir))
    			{
    				System.out.println("Created new transferfile: " + tmpPath);
    			}
        	}
    	}
    	else if(dir.toString().toLowerCase().endsWith(transfer_status_file.toLowerCase()))
    	{
    		BufferedReader br = new BufferedReader(new FileReader(dir.toString()));
    		try {
    		    //StringBuilder sb = new StringBuilder();
    		    String line = br.readLine();

    		    while (line != null) {
    		        //sb.append(line);
    		        //sb.append(System.lineSeparator());
    		    	if(line.contains("="))
    		        {
    		        	String[] sline = line.split("=");
    		        	if(sline[0].toLowerCase().equals(statusString))
    		        	{
    		        		if(sline[1].toLowerCase().equals("yes"))
    		        		{
    		        			status = "yes";
    		        			System.out.println("Status : " + statusString + "=" + status);
    		        		} 
    		        	}		
    		        }
    		    	line = br.readLine();
    		    }
    		    //String everything = sb.toString();
    		} finally {
    		    br.close();
    		}
    	}
    	}
    	catch(Exception ex)
    	{
    		System.out.println("InPathPreProcessor Error : transferStatus : " + ex.toString());
    	}
    	return status;
    }

	private boolean createTransferFile(Path dir)
    {
		boolean isTransfer = false;
		try
    	{	/*	
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
			
    	}
    	catch(Exception ex)
    	{
    		System.out.println("PathPreProcessor createTransferFile Error : " + ex.toString());
    	}
    	return isTransfer;
    }
		
}



