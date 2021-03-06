package com.researchworx.genomics.gobjectingestion.plugincore;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.researchworx.genomics.gobjectingestion.folderprocessor.InPathPreProcessor;
import com.researchworx.genomics.gobjectingestion.folderprocessor.InPathProcessor;
import com.researchworx.genomics.gobjectingestion.folderprocessor.OutPathPreProcessor;
import com.researchworx.genomics.gobjectingestion.folderprocessor.OutPathProcessor;
import com.researchworx.genomics.gobjectingestion.folderprocessor.WatchDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PluginEngine {
    private static final Logger logger = LoggerFactory.getLogger(PluginEngine.class);
    private static String watchDirectoryName;

    public static ConcurrentLinkedQueue<Path> pathQueue;
    public static boolean PathProcessorActive = false;
    public static Config config;

    public static void main(String[] args) throws Exception {
        logger.info("Checking configuration");
        String configFile = checkConfig(args);

        logger.debug("Generating new [Config]");
        config = new Config(configFile);
        logger.trace("Building new ConcurrentLinkedQueue");
        pathQueue = new ConcurrentLinkedQueue<>();

        logger.trace("Provisioning uninstantiated ppThread with null");
        Thread ppThread = null;

        logger.trace("Grabbing [pathstage] from config");
        int pathStage = Integer.parseInt(config.getParam("pathstage"));
        logger.debug("[pathStage] == {}", pathStage);
        logger.info("Building Stage [{}]", pathStage);
        switch (pathStage) {
            case 1:
                logger.trace("Grabbing [pathstage1 --> watchdirectory] string and setting to [watchDirectoryName]");
                watchDirectoryName = config.getParam("pathstage1", "watchdirectory");
                logger.debug("Generating new [InPathPreProcessor] runnable");
                InPathPreProcessor ippp = new InPathPreProcessor();
                logger.trace("Building ppThread around new [InPathPreProcessor] runnable");
                ppThread = new Thread(ippp);
                break;
            case 2:
                logger.debug("Generating new [OutPathPreProcessor] runnable");
                OutPathPreProcessor oppp = new OutPathPreProcessor();
                logger.trace("Building ppThread around new [OutPathPreProcessor] runnable");
                ppThread = new Thread(oppp);
                break;
            case 3:
                logger.info("Grabbing [pathstage3 --> watchdirectory] string and setting to [watchDirectoryName]");
                watchDirectoryName = config.getParam("pathstage3", "watchdirectory");
                logger.info("WatchDirectoryName=" + watchDirectoryName);
                logger.info("Generating new [InPathProcessor] runnable");
                InPathProcessor pp = new InPathProcessor();
                logger.info("Building ppThread around new [InPathProcessor] runnable");
                ppThread = new Thread(pp);
                break;
            case 4:
                logger.debug("Generating new [OutPathProcessor] runnable");
                OutPathProcessor opp = new OutPathProcessor();
                logger.trace("Building pThread around new [OutPathProcessor] runnable");
                ppThread = new Thread(opp);
                break;
            case 5:
                //String command = "docker run -t -v /home/gpackage:/gpackage -v /home/gdata/input/160427_D00765_0033_AHKM2CBCXX/Sample3:/gdata/input -v /home/gdata/output/f8de921b-fdfa-4365-bf7d-39817b9d1883:/gdata/output  intrepo.uky.edu:5000/gbase /gdata/input/commands_main.sh";
                //System.out.println(command);
                //executeCommand(command);
                //test();
                break;
            default:
                logger.trace("Encountered default switch path");
                break;
        }
        logger.trace("Checking to ensure that ppThread has been instantiated");
        if (ppThread == null) {
            logger.error("PreProcessing Thread failed to generate, exiting...");
            return;
        }
        logger.info("Starting Stage [{}] Object Ingestion");
        ppThread.start();

        logger.trace("Checking [watchDirectoryName] for null");
        if (watchDirectoryName != null) {
            logger.trace("Grabbing path for [watchDirectoryName]");
            Path dir = Paths.get(watchDirectoryName);
            logger.trace("Instantiating new [WatchDirectory] from [watchDirectoryName] path");
            WatchDirectory wd = new WatchDirectory(dir, true);
            logger.trace("Starting Directory Watcher");
            wd.processEvents();
        }
    }

    /*
    private static void test() {

        DockerClientConfig config = DockerClientConfig.createDefaultConfigBuilder()
                .withDockerHost("tcp://localhost:2376")
                .withDockerTlsVerify(false)
                .withDockerCertPath("/home/user/.docker/certs")
                .withDockerConfig("/home/user/.docker")
                .withApiVersion("1.9.1")
                .withRegistryUrl("https://intrepo.uky.edu:5000/v2/")
                .withRegistryUsername("genomicuser")
                .withRegistryPassword("u$secure01")
                .withRegistryEmail("dockeruser@github.com")
                .build();
        DockerClient docker = DockerClientBuilder.getInstance(config).build();
        for(Image im : docker.listImagesCmd().exec()) {
            System.out.println(im.toString() + " " + im.getId() + " " + im.getSize());
        }
        System.out.println(docker.listImagesCmd().toString());
    }
    */

    private static String checkConfig(String[] args) {
        logger.trace("Stepping into checkConfig method");
        String jarName = new java.io.File(PluginEngine.class.getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .getPath())
                .getName();
        logger.trace("jarName = {}", jarName);
        String name = jarName.replace(".jar","");
        logger.trace("name = {}", name);
        String errorMsg = name + "\n" +
                "Usage: java -jar " +
                jarName + " -f <configuration_file>\n";
        logger.trace("errorMsg = {}", errorMsg);
        if (args.length != 2) {
            System.err.println("ERROR: Invalid number of arguments.");
            System.err.println(errorMsg);
            System.exit(1);
        } else if (!args[0].equals("-f")) {
            System.err.println("ERROR: Must specify configuration file.");
            System.err.println(errorMsg);
            System.exit(1);
        } else {
            File f = new File(args[1]);
            if (!f.exists()) {
                System.err.println("The specified configuration file: " + args[1] + " does not exist");
                System.exit(1);
            }
        }
        return args[1];
    }
}
