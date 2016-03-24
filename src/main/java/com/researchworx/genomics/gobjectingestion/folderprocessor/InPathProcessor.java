package com.researchworx.genomics.gobjectingestion.folderprocessor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;


import com.researchworx.genomics.gobjectingestion.objectstorage.ObjectEngine;
import com.researchworx.genomics.gobjectingestion.plugincore.PluginEngine;

public class InPathProcessor implements Runnable {

    private String transferStubFile;
    private String bucket_name;

    public InPathProcessor() {
        transferStubFile = PluginEngine.config.getParam("transferfile");
        bucket_name = PluginEngine.config.getParam("s3", "bucket");
    }

    public void run() {
        try {
            PluginEngine.PathProcessorActive = true;
            while (PluginEngine.PathProcessorActive) {
                try {
                    //check if we should process folder
                    Path dir = PluginEngine.pathQueue.poll();
                    if (dir != null) {
                        //if transfer file exist
                        String status = transferStatus(dir, "transfer_ready_status");
                        if (status != null) {
                            if (status.equals("yes")) {
                                //process dir
                                processDir(dir);
                            }
                        }
                    } else {
                        Thread.sleep(1000);
                    }
                } catch (Exception ex) {
                    System.out.println("PathProcessorActive Error: " + ex.toString());
                }
            }
        } catch (Exception ex) {
            System.out.println("PathProcessor Error: " + ex.toString());
        }
    }

    private void processDir(Path dir) {
        String inDir = dir.toString();
        inDir = inDir.substring(0, inDir.length() - transferStubFile.length() - 1);

        String outDir = inDir;
        outDir = outDir.substring(outDir.lastIndexOf("/") + 1, outDir.length());

        ObjectEngine oe = new ObjectEngine("pathstage3");
        String status = transferStatus(dir, "transfer_complete_status");

        if (status.equals("no")) {
            if (oe.uploadDirectory(bucket_name, inDir, outDir)) {

                if (setTransferFile(dir)) {
                    System.out.println("Directory Transfered inDir=" + inDir + " outDir=" + outDir);
                } else {
                    System.out.println("Directory Transfer Failed inDir=" + inDir + " outDir=" + outDir);
                }
            }
        } else if (status.equals("yes")) {
            List<String> filterList = new ArrayList<>();
            filterList.add(transferStubFile);
            if (oe.isSyncDir(bucket_name, outDir, inDir, filterList)) {
                System.out.println("Directory Sycned inDir=" + inDir + " outDir=" + outDir);
            }
        }
    }

    private boolean setTransferFile(Path dir) {
        boolean isSet = false;

        try {
            //transfer_request_flag.txt
            if (dir.endsWith(transferStubFile)) {
                //BufferedReader br = null;
                //BufferedWriter bw = null;
                //File xferFile = null;
                List<String> slist = new ArrayList<>();
                try (BufferedReader br = new BufferedReader(new FileReader(dir.toString()))) {
                    //slist = new ArrayList<>();
                    //br = new BufferedReader(new FileReader(dir.toString()));
                    //StringBuilder sb = new StringBuilder();
                    String line = br.readLine();

                    while (line != null) {
                        //sb.append(line);
                        //sb.append(System.lineSeparator());
                        if (line.contains("=")) {
                            String[] sline = line.split("=");
                            if (sline[0].toLowerCase().equals("transfer_complete_status")) {
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
                    for (String line : slist) {
                        bw.write(line + "\n");
                    }
                } /*finally {
                    bw.close();
				}*/
                String status = transferStatus(dir, "transfer_complete_status");
                if (status.equals("yes")) {
                    isSet = true;
                }
            }
        } catch (Exception ex) {
            System.out.println("PathProcessor Error : setTransferFile : " + ex.toString());
        }
        return isSet;
    }

    private String transferStatus(Path dir, String statusString) {
        String status = null;
        try {
            //transfer_request_flag.txt
            if (dir.endsWith(transferStubFile)) {
                //BufferedReader br = new BufferedReader(new FileReader(dir.toString()));
                try (BufferedReader br = new BufferedReader(new FileReader(dir.toString()))) {
                    //StringBuilder sb = new StringBuilder();
                    String line = br.readLine();

                    while (line != null) {
                        //sb.append(line);
                        //sb.append(System.lineSeparator());
                        if (line.contains("=")) {
                            String[] sline = line.split("=");
                            if (sline[0].toLowerCase().equals(statusString)) {
                                if (sline[1].toLowerCase().equals("no")) {
                                    //transfer_complete_status=no
                                    status = "no";
                                } else if (sline[1].toLowerCase().equals("yes")) {
                                    //transfer_complete_status=no
                                    status = "yes";
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
        } catch (Exception ex) {
            System.out.println("PathProcessor Error : transferStatus : " + ex.toString());
        }
        return status;
    }
}



