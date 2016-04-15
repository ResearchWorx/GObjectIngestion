package com.researchworx.genomics.gobjectingestion.folderprocessor;

import java.io.*;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InPathProcessor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(InPathPreProcessor.class);

    private final String transfer_watch_file;
    private final String transfer_status_file;
    private final String bucket_name;

    public InPathProcessor() {
        logger.trace("InPathProcessor instantiated");
        transfer_watch_file = PluginEngine.config.getParam("pathstage3", "transfer_watch_file");
        logger.debug("\"pathstage3\" --> \"transfer_watch_file\" from config [{}]", transfer_watch_file);
        transfer_status_file = PluginEngine.config.getParam("pathstage3", "transfer_status_file");
        logger.debug("\"pathstage3\" --> \"transfer_status_file\" from config [{}]", transfer_status_file);
        bucket_name = PluginEngine.config.getParam("pathstage3", "bucket");
        logger.debug("\"pathstage3\" --> \"bucket_name\" from config [{}]", bucket_name);
    }

    @Override
    public void run() {
        logger.trace("Thread starting");
        try {
            logger.trace("Setting [PathProcessorActive] to true");
            PluginEngine.PathProcessorActive = true;
            ObjectEngine oe = new ObjectEngine("pathstage1");
            logger.trace("Issuing [ObjectEngine].createBucket using [bucket_name = {}]", bucket_name);
            oe.createBucket(bucket_name);
            logger.trace("Entering while-loop");
            while (PluginEngine.PathProcessorActive) {
                try {
                    Path dir = PluginEngine.pathQueue.poll();
                    if (dir != null) {
                        logger.info("Processing folder [{}]", dir);
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
        String status = "no";
        try {
            if (dir.toString().toLowerCase().endsWith(transfer_watch_file.toLowerCase())) {
                logger.trace("[dir] tail matches [transfer_watch_file]");
                logger.trace("Replacing [transfer_watch_file] with [transfer_status_file]");
                String tmpPath = dir.toString().replace(transfer_watch_file, transfer_status_file);
                logger.debug("Creating file [{}]", tmpPath);
                File f = new File(tmpPath);
                if (!f.exists()) {
                    logger.trace("File doesn't already exist");
                    if (createTransferFile(dir)) {
                        logger.info("Created new transferfile: " + tmpPath);
                    }
                }
            } else if (dir.toString().toLowerCase().endsWith(transfer_status_file.toLowerCase())) {
                logger.trace("[dir] tail matches [transfer_status_file]");
                try (BufferedReader br = new BufferedReader(new FileReader(dir.toString()))) {
                    logger.trace("Reading line from [transfer_status_file]");
                    String line = br.readLine();
                    while (line != null) {
                        if (line.contains("=")) {
                            logger.trace("Line contains \"=\"");
                            String[] sline = line.split("=");
                            logger.debug("Line split into {} and {}", sline[0], sline[1]);
                            if (sline[0].toLowerCase().equals(statusString) && sline[1].toLowerCase().equals("yes")) {
                                status = "yes";
                                logger.info("Status: {}={}", statusString, status);
                            }
                        }
                        line = br.readLine();
                    }
                }
            }
        } catch (Exception ex) {
            logger.error("transferStatus : {}", ex.toString());
        }
        return status;
    }

    private boolean createTransferFile(Path dir) {
        logger.debug("Call to createTransferFile [dir = {}]", dir);
        boolean isTransfer = false;
        try {
            logger.trace("Building file path");
            String tmpPath = dir.toString().replace(transfer_watch_file, transfer_status_file);
            logger.trace("Building lines array");
            List<String> lines = Arrays.asList("TRANSFER_READY_STATUS=YES", "TRANSFER_COMPLETE_STATUS=NO");
            logger.debug("[tmpPath = {}]", tmpPath);
            Path file = Paths.get(tmpPath);
            logger.trace("Writing lines to file at [tmpPath]");
            Files.write(file, lines, Charset.forName("UTF-8"));
            logger.trace("Completed writing to file");
            isTransfer = true;
        } catch (Exception ex) {
            logger.error("createTransferFile Error : {}", ex.getMessage());
        }
        return isTransfer;
    }

    private void processDir(Path dir) {
        logger.debug("Call to processDir [dir = {}]", dir);

        String inDir = dir.toString();
        inDir = inDir.substring(0, inDir.length() - transfer_status_file.length() - 1);
        logger.debug("[inDir = {}]", inDir);

        String outDir = inDir;
        outDir = outDir.substring(outDir.lastIndexOf("/") + 1, outDir.length());
        logger.debug("[outDir = {}]", outDir);

        logger.info("Start processing directory {}", outDir);

        ObjectEngine oe = new ObjectEngine("pathstage1");
        String status = transferStatus(dir, "transfer_complete_status");
        List<String> filterList = new ArrayList<>();
        logger.trace("Adding [transfer_status_file] to [filterList]");
        filterList.add(transfer_status_file);

        if (status.equals("no")) {
            logger.debug("[status = \"no\"]");
            Map<String, String> md5map = oe.getDirMD5(inDir, filterList);
            logger.trace("Setting MD5 hash");
            setTransferFileMD5(dir, md5map);
            logger.trace("Transferring directory");
            if (oe.uploadDirectory(bucket_name, inDir, outDir)) {
                if (setTransferFile(dir)) {
                    logger.debug("Directory Transfered [inDir = {}, outDir = {}]", inDir, outDir);
                } else {
                    logger.error("Directory Transfer Failed [inDir = {}, outDir = {}]", inDir, outDir);
                }
            }
        } else if (status.equals("yes")) {
            logger.trace("[status = \"yes\"]");
            if (oe.isSyncDir(bucket_name, outDir, inDir, filterList)) {
                logger.debug("Directory Sycned inDir={} outDir={}", inDir, outDir);
            }
        }
    }

    private void setTransferFileMD5(Path dir, Map<String, String> md5map) {
        logger.debug("Call to setTransferFileMD5 [dir = {}, md5map = {}", dir, md5map.toString());
        try {
            String watchDirectoryName = PluginEngine.config.getParam("pathstage1", "watchdirectory");
            logger.debug("Grabbing [pathstage1 --> watchdirectory] from config [{}]", watchDirectoryName);
            if (dir.toString().toLowerCase().endsWith(transfer_status_file.toLowerCase())) {
                logger.trace("[dir] ends with [transfer_status_file]");
                PrintWriter out = null;
                try {
                    logger.trace("Opening [dir] to write");
                    out = new PrintWriter(new BufferedWriter(new FileWriter(dir.toString(), true)));
                    for (Map.Entry<String, String> entry : md5map.entrySet()) {
                        String md5file = entry.getKey().replace(watchDirectoryName, "");
                        if (md5file.startsWith("/")) {
                            md5file = md5file.substring(1);
                        }
                        out.write(md5file + ":" + entry.getValue() + "\n");
                        logger.debug("[md5file = {}, entry = {}] written", md5file, entry.getValue());
                    }
                } finally {
                    try {
                        assert out != null;
                        out.flush();
                        out.close();
                    } catch (AssertionError e) {
                        logger.error("setTransferFileMd5 - PrintWriter was pre-emptively shutdown");
                    }
                }
            }
        } catch (Exception ex) {
            logger.error("setTransferFile : {}", ex.getMessage());
        }
    }

    private boolean setTransferFile(Path dir) {
        logger.debug("Call to setTransferFile [dir = {}]");
        boolean isSet = false;
        try {
            if (dir.toString().toLowerCase().endsWith(transfer_status_file.toLowerCase())) {
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
                try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(dir.toString()).toString()))) {
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