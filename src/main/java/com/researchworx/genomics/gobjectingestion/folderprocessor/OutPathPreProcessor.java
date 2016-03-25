package com.researchworx.genomics.gobjectingestion.folderprocessor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.researchworx.genomics.gobjectingestion.objectstorage.ObjectEngine;
import com.researchworx.genomics.gobjectingestion.plugincore.PluginEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutPathPreProcessor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(OutPathPreProcessor.class);

    private final String transfer_watch_file;
    private final String transfer_status_file;
    private final String incoming_directory;
    private final String bucket_name;

    public OutPathPreProcessor() {
        logger.debug("OutPathPreProcessor Instantiated");
        transfer_watch_file = PluginEngine.config.getParam("pathstage2", "transfer_watch_file");
        logger.debug("\"pathstage2\" --> \"transfer_watch_file\" from config [{}]", transfer_watch_file);
        transfer_status_file = PluginEngine.config.getParam("pathstage2", "transfer_status_file");
        logger.debug("\"pathstage2\" --> \"transfer_status_file\" from config [{}]", transfer_status_file);
        incoming_directory = PluginEngine.config.getParam("pathstage2", "incoming_directory");
        logger.debug("\"pathstage2\" --> \"incoming_directory\" from config [{}]", incoming_directory);
        bucket_name = PluginEngine.config.getParam("pathstage2", "bucket");
        logger.debug("\"pathstage2\" --> \"bucket\" from config [{}]", bucket_name);
    }

    @Override
    public void run() {
        logger.trace("Thread starting");
        try {
            logger.trace("Setting [PathProcessorActive] to true");
            PluginEngine.PathProcessorActive = true;
            ObjectEngine oe = new ObjectEngine("pathstage2");
            logger.trace("Entering while-loop");
            while (PluginEngine.PathProcessorActive) {
                try {
                    //oe.deleteBucketContents(bucket_name);
                    logger.trace("Populating [remoteDirs]");
                    List<String> remoteDirs = oe.listBucketDirs(bucket_name);
                    logger.trace("Populating [localDirs]");
                    List<String> localDirs = getWalkPath(incoming_directory);

                    List<String> newDirs = new ArrayList<>();
                    for (String remoteDir : remoteDirs) {
                        if (!localDirs.contains(remoteDir)) {
                            if (oe.doesObjectExist(bucket_name, remoteDir + transfer_watch_file)) {
                                logger.debug("Adding [remoteDir = {}] to [newDirs]", remoteDir);
                                newDirs.add(remoteDir);
                            }
                        }
                    }
                    if (!newDirs.isEmpty()) {
                        logger.trace("[newDirs] has buckets to process");
                        processBucket(newDirs);
                    }
                    Thread.sleep(30000);
                } catch (Exception ex) {
                    logger.error("run : while {}", ex.getMessage());
                }
            }
        } catch (Exception ex) {
            logger.error("run {}", ex.getMessage());
        }
    }

    private void processBucket(List<String> newDirs) {
        logger.debug("Call to processBucket [newDir = {}]", newDirs.toString());
        ObjectEngine oe = new ObjectEngine("pathstage2");

        for (String remoteDir : newDirs) {
            logger.debug("Downloading directory {} to [incoming_directory]", remoteDir);
            oe.downloadDirectory(bucket_name, remoteDir, incoming_directory);

            List<String> filterList = new ArrayList<>();
            logger.trace("Add [transfer_status_file] to [filterList]");
            filterList.add(transfer_status_file);
            String inDir = incoming_directory;
            if (!inDir.endsWith("/")) {
                inDir = inDir + "/";
            }
            inDir = inDir + remoteDir;
            logger.debug("[inDir = {}]", inDir);
            oe = new ObjectEngine("pathstage2");
            if (oe.isSyncDir(bucket_name, remoteDir, inDir, filterList)) {
                logger.debug("Directory Sycned [inDir = {}]", inDir);
                Map<String, String> md5map = oe.getDirMD5(inDir, filterList);
                logger.trace("Set MD5 hash");
                setTransferFileMD5(inDir + transfer_status_file, md5map);
            }
        }
    }

    private void setTransferFileMD5(String dir, Map<String, String> md5map) {
        logger.debug("Call to setTransferFileMD5 [dir = {}, md5map = {}]", dir, md5map);
        try {
            PrintWriter out = null;
            try {
                logger.trace("Opening [dir] to write");
                out = new PrintWriter(new BufferedWriter(new FileWriter(dir, true)));
                for (Map.Entry<String, String> entry : md5map.entrySet()) {
                    String md5file = entry.getKey().replace(incoming_directory, "");
                    if (md5file.startsWith("/")) {
                        md5file = md5file.substring(1);
                    }
                    out.write(md5file + ":" + entry.getValue() + "\n");
                    logger.debug("[md5file = {}, entry = {}] written", md5file, entry.getValue());
                }
            } finally {
                assert out != null;
                out.flush();
                out.close();
            }
        } catch (Exception ex) {
            logger.error("setTransferFile {}", ex.getMessage());
        }
    }

    private List<String> getWalkPath(String path) {
        logger.debug("Call to getWalkPath [path = {}]", path);
        if (!path.endsWith("/")) {
            path = path + "/";
        }
        List<String> dirList = new ArrayList<>();

        File root = new File(path);
        File[] list = root.listFiles();

        if (list == null) {
            logger.trace("[list] is null, returning [dirList (empty array)]");
            return dirList;
        }

        for (File f : list) {
            if (f.isDirectory()) {
                //walkPath( f.getAbsolutePath() );
                String dir = f.getAbsolutePath().replace(path, "");
                logger.debug("Adding \"{}/\" to [dirList]", dir);
                dirList.add(dir + "/");
            }
        }
        return dirList;
    }
}



