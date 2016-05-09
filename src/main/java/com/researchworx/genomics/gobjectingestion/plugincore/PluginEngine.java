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

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.ListImagesCmd;
import com.github.dockerjava.api.model.Image;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
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
                String command = "docker run -t -v /home/gpackage:/gpackage -v /home/gdata/input/160427_D00765_0033_AHKM2CBCXX/Sample3:/gdata/input -v /home/gdata/output/f8de921b-fdfa-4365-bf7d-39817b9d1883:/gdata/output  intrepo.uky.edu:5000/gbase /gdata/input/commands_main.sh";
                System.out.println(command);
                executeCommand(command);
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

    private static void executeCommand(String command) {
        StringBuffer output = new StringBuffer();
        StringBuffer error = new StringBuffer();
        Process p;
        try {
            p = Runtime.getRuntime().exec(command);

            BufferedReader outputFeed = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String outputLine;
            while ((outputLine = outputFeed.readLine()) != null) {
                output.append(outputLine);

                String[] outputStr = outputLine.split(":");

                for(String str : outputStr) {
                    System.out.println(outputStr.length + " " + str);
                }
                /*
                if(outputStr.length == 5) {
                    Calendar cal = Calendar.getInstance();
                    SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.US);
                    cal.setTime(sdf.parse(outputStr[1]));// all done
                    long logdiff = (System.currentTimeMillis() - cal.getTimeInMillis());
                    if(outputStr[0].toLowerCase().equals("info")) {
                        logger.info("Log diff = " + logdiff + " : " +  outputStr[2] + " : " + outputStr[3] + " : " + outputStr[4]);
                    }
                    else if (outputStr[0].toLowerCase().equals("error")) {
                        logger.error("Pipeline Error : " + output.toString());
                    }
                }
                else {
                    logger.error(outputStr.length + " Invalid output format: " + output.toString());
                }
                */
                //logger.info(outputLine);
            }

            if (!output.toString().equals("")) {
                //INFO : Mon May  9 20:35:42 UTC 2016 : UKHC Genomics pipeline V-1.0 : run_secondary_analysis.pl : Module Function run_locally() - execution successful
                logger.info(output.toString());
                //    clog.info(output.toString());
            }
            BufferedReader errorFeed = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            String errorLine;
            while ((errorLine = errorFeed.readLine()) != null) {
                error.append(errorLine);
                logger.error(errorLine);
            }

            if (!error.toString().equals(""))
                logger.error(error.toString());
            //    clog.error(error.toString());
            p.waitFor();

        } catch (IOException ioe) {
            // WHAT!?! DO SOMETHIN'!
        } catch (InterruptedException ie) {
            // WHAT!?! DO SOMETHIN'!
        } catch (Exception e) {
            // WHAT!?! DO SOMETHIN'!
        }
    }

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
