package com.researchworx.genomics.gobjectingestion.folderprocessor;

import com.researchworx.genomics.gobjectingestion.objectstorage.ObjectEngine;
import com.researchworx.genomics.gobjectingestion.plugincore.PluginEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class OutPathProcessor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(OutPathProcessor.class);

    private final String transfer_watch_file;
    private final String transfer_status_file;
    private final String incoming_directory;
    private final String outgoing_directory;
    private final String bucket_name;

    public OutPathProcessor() {
        logger.debug("OutPathPreProcessor Instantiated");
        transfer_watch_file = PluginEngine.config.getParam("pathstage4", "transfer_watch_file");
        logger.debug("\"pathstage4\" --> \"transfer_watch_file\" from config [{}]", transfer_watch_file);
        transfer_status_file = PluginEngine.config.getParam("pathstage4", "transfer_status_file");
        logger.debug("\"pathstage4\" --> \"transfer_status_file\" from config [{}]", transfer_status_file);
        incoming_directory = PluginEngine.config.getParam("pathstage4", "incoming_directory");
        logger.debug("\"pathstage4\" --> \"incoming_directory\" from config [{}]", incoming_directory);
        outgoing_directory = PluginEngine.config.getParam("pathstage4", "outgoing_directory");
        logger.debug("\"pathstage4\" --> \"outgoing_directory\" from config [{}]", outgoing_directory);
        bucket_name = PluginEngine.config.getParam("pathstage4", "bucket");
        logger.debug("\"pathstage4\" --> \"bucket\" from config [{}]", bucket_name);
    }

    @Override
    public void run() {
        logger.trace("Thread starting");
        try {
            logger.trace("Setting [PathProcessorActive] to true");
            PluginEngine.PathProcessorActive = true;
            ObjectEngine oe = new ObjectEngine("pathstage4");
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
        ObjectEngine oe = new ObjectEngine("pathstage4");

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
            oe = new ObjectEngine("pathstage4");
            if (oe.isSyncDir(bucket_name, remoteDir, inDir, filterList)) {
                logger.debug("Directory Sycned [inDir = {}]", inDir);
                Map<String, String> md5map = oe.getDirMD5(inDir, filterList);
                logger.trace("Set MD5 hash");
                setTransferFileMD5(inDir + transfer_status_file, md5map);
                //process sample directories
                processDirectories(inDir);
            }

        }
    }

    private boolean deleteDirectory(File path) {
        if( path.exists() ) {
            File[] files = path.listFiles();
            for(int i=0; i<files.length; i++) {
                if(files[i].isDirectory()) {
                    deleteDirectory(files[i]);
                }
                else {
                    files[i].delete();
                }
            }
        }
        return( path.delete() );
    }

    private void processDirectories(String dir)
    {
        logger.trace("Processing Directory : " + dir);
        File file = new File(dir);
        String[] directories = file.list(new FilenameFilter() {
            @Override
            public boolean accept(File current, String name) {
                return new File(current, name).isDirectory();
            }
        });

        for(String subDir : directories){
            logger.trace("Processing SubDirectory : " + subDir);
            String commands_main_filename = dir + subDir + "/commands_main.sh";
            String config_files_directoryname = dir + subDir + "/config_files";
            File commands_main = new File(commands_main_filename);
            File config_files = new File(config_files_directoryname);

            if(commands_main.exists() && !commands_main.isDirectory() && config_files.exists() && config_files.isDirectory()) {
                // do something
                logger.trace("Found : " + commands_main_filename + " and " + config_files_directoryname);

                UUID id = UUID.randomUUID(); //create random tmp location
                String tmpInput = dir + "/" + subDir;
                String tmpoutput = outgoing_directory + "/" + id.toString();
                File tmpoutputdir = new File(tmpoutput);
                if(commands_main.exists()) {
                    deleteDirectory(tmpoutputdir);
                }
                tmpoutputdir.mkdir();

                logger.trace("Creating tmp output location : " + tmpoutput);

                logger.info("Launching processing container:");
                logger.info("Input Location: "  + tmpInput);
                logger.info("Output Location: " + tmpoutput);
                //upload data

                //cleanup
                logger.trace("Removing tmp output location : " + tmpoutput);
                deleteDirectory(tmpoutputdir);
            }
            else {
                logger.error("Skipping! : commands_main.sh and config_files not found in subdirectory " + dir + "/" + subDir);
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
                try {
                    assert out != null;
                    out.flush();
                    out.close();
                } catch (AssertionError e) {
                    logger.error("setTransferFileMd5 - PrintWriter was pre-emptively shutdown");
                }
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



