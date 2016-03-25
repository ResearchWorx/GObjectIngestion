package com.researchworx.genomics.gobjectingestion.folderprocessor;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;


import com.researchworx.genomics.gobjectingestion.objectstorage.ObjectEngine;
import com.researchworx.genomics.gobjectingestion.plugincore.PluginEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InPathProcessor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(InPathProcessor.class);

    private final String transferStubFile;
    private final String bucket_name;

    public InPathProcessor() {
        logger.trace("InPathProcessor instantiated");
        transferStubFile = PluginEngine.config.getParam("transferfile");
        logger.debug("Grabbed \"transferfile\" param from config [transferStubFile = {}]", transferStubFile);
        bucket_name = PluginEngine.config.getParam("s3", "bucket");
        logger.debug("Grabbed \"s3\" --> \"bucket\" from config [bucket_name = {}]");
    }

    @Override
    public void run() {
        logger.trace("Thread starting");
        try {
            logger.trace("Setting [PathProcessorActive] to true and entering while-loop");
            PluginEngine.PathProcessorActive = true;
            while (PluginEngine.PathProcessorActive) {
                try {
                    Path dir = PluginEngine.pathQueue.poll();
                    if (dir != null) {
                        String status = transferStatus(dir, "transfer_ready_status");
                        if (status != null && status.equals("yes")) {
                            logger.trace("Transfer file exists, processing");
                            processDir(dir);
                        }
                    } else {
                        Thread.sleep(1000);
                    }
                } catch (Exception ex) {
                    logger.error("run : while {}", ex.getMessage());
                }
            }
        } catch (Exception ex) {
            logger.error("run {}", ex.getMessage());
        }
    }

    private String transferStatus(Path dir, String statusString) {
        logger.debug("Call to transferStatus [dir = {}, statusString = {}]", dir, statusString);
        String status = null;
        try {
            if (dir.toString().toLowerCase().endsWith(transferStubFile.toLowerCase())) {
                logger.trace("[dir] tail matches [transferStubFile]");
                try (BufferedReader br = new BufferedReader(new FileReader(dir.toString()))) {
                    logger.trace("Reading line from [transferStubFile]");
                    String line = br.readLine();

                    while (line != null) {
                        if (line.contains("=")) {
                            logger.trace("Line contains \"=\"");
                            String[] sline = line.split("=");
                            logger.debug("Line split into {} and {}", sline[0], sline[1]);
                            if (sline[0].toLowerCase().equals(statusString)) {
                                if (sline[1].toLowerCase().equals("no")) {
                                    status = "no";
                                } else if (sline[1].toLowerCase().equals("yes")) {
                                    //transfer_complete_status=no
                                    status = "yes";
                                }
                            }
                        }
                        line = br.readLine();
                    }
                }
            }
        } catch (Exception ex) {
            logger.error("transferStatus {}", ex.getMessage());
        }
        return status;
    }

    private void processDir(Path dir) {
        logger.debug("Call to processDir [dir = {}]", dir);

        String inDir = dir.toString();
        inDir = inDir.substring(0, inDir.length() - transferStubFile.length() - 1);
        logger.debug("[inDir = {}]", inDir);

        String outDir = inDir;
        outDir = outDir.substring(outDir.lastIndexOf("/") + 1, outDir.length());
        logger.debug("[outDir = {}]", outDir);

        logger.info("Start processing directory {}", outDir);

        ObjectEngine oe = new ObjectEngine("pathstage3");
        String status = transferStatus(dir, "transfer_complete_status");

        if (status.equals("no")) {
            logger.debug("[status = \"no\"]");
            if (oe.uploadDirectory(bucket_name, inDir, outDir)) {
                if (setTransferFile(dir, transferStubFile)) {
                    logger.debug("Directory Transfered [inDir = {}, outDir = {}]", inDir, outDir);
                } else {
                    logger.debug("Directory Transfer Failed [inDir = {}, outDir = {}]", inDir, outDir);
                }
            }
        } else if (status.equals("yes")) {
            logger.trace("[status = \"yes\"]");
            List<String> filterList = new ArrayList<>();
            filterList.add(transferStubFile);
            if (oe.isSyncDir(bucket_name, outDir, inDir, filterList)) {
                logger.debug("Directory Sycned [inDir = {}, outDir = {}]", inDir, outDir);
            }
        }
    }

    private boolean setTransferFile(Path dir, String file) {
        logger.debug("Call to setTransferFile [dir = {}]");
        boolean isSet = false;
        try {
            if (dir.toString().toLowerCase().endsWith(file.toLowerCase())) {
                logger.trace("[dir] ends with [transfer_status_file]");
                List<String> slist = new ArrayList<>();
                try (BufferedReader br = new BufferedReader(new FileReader(dir.toString()))) {
                    String line = br.readLine();
                    logger.trace("Grabbing a line from [dir]");
                    while (line != null) {
                        if (line.contains("=")) {
                            logger.trace("Line contains \"=\"");
                            String[] sline = line.split("=");
                            logger.debug("Line split into {} and {}", sline[0], sline[1]);
                            if (sline[0].toLowerCase().equals("transfer_complete_status")) {
                                logger.trace("[sline[0] == \"transfer_complete_status\"]");
                                slist.add("TRANSFER_COMPLETE_STATUS=YES");
                            } else {
                                logger.trace("[sline[0] != \"transfer_complete_status\"]");
                                slist.add(line);
                            }
                        }
                        line = br.readLine();
                    }
                }
                try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(dir.toString())))) {
                    logger.trace("Writing to [dir]");
                    for (String line : slist) {
                        bw.write(line + "\n");
                    }
                }
                logger.trace("Updating status to complete");
                String status = transferStatus(dir, "transfer_complete_status");
                if (status.equals("yes")) {
                    isSet = true;
                }
            }
        } catch (Exception ex) {
            logger.error("setTransferFile {}", ex.getMessage());
        }
        return isSet;
    }
}



