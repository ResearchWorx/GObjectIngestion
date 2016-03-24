package com.researchworx.genomics.gobjectingestion.plugincore;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.researchworx.genomics.gobjectingestion.folderprocessor.InPathPreProcessor;
import com.researchworx.genomics.gobjectingestion.folderprocessor.InPathProcessor;
import com.researchworx.genomics.gobjectingestion.folderprocessor.OutPathPreProcessor;
import com.researchworx.genomics.gobjectingestion.folderprocessor.WatchDirectory;

public class PluginEngine {
    private static String watchDirectoryName;

    public static ConcurrentLinkedQueue<Path> pathQueue;
    public static boolean PathProcessorActive = false;
    public static Config config;

    public static void main(String[] args) throws Exception {
        //Make sure initial input is sane.
        String configFile = checkConfig(args);

        //Make sure config file
        config = new Config(configFile);
        pathQueue = new ConcurrentLinkedQueue<>();

        Thread ppThread = null;

        int pathStage = Integer.parseInt(config.getParam("pathstage"));
        switch (pathStage) {
            case 1:
                watchDirectoryName = config.getParam("pathstage1", "watchdirectory");
                InPathPreProcessor ippp = new InPathPreProcessor();
                ppThread = new Thread(ippp);
                break;
            case 2:
                OutPathPreProcessor oppp = new OutPathPreProcessor();
                ppThread = new Thread(oppp);
                break;
            case 3:
                watchDirectoryName = config.getParam("pathstage3", "watchdirectory");
                InPathProcessor pp = new InPathProcessor();
                ppThread = new Thread(pp);

            default:
                break;
        }
        if (ppThread == null) {
            System.out.println("PreProcessing Thread failed to generate, exiting...");
            return;
        }
        ppThread.start();

        if (watchDirectoryName != null) {
            Path dir = Paths.get(watchDirectoryName);
            WatchDirectory wd = new WatchDirectory(dir, true);
            wd.processEvents();
        }
    }

    private static String checkConfig(String[] args) {
        String errorMgs = "GObjectWatcher\n" +
                "Usage: java -jar GObjectWatcher.jar" +
                " -f <configuration_file>\n";

        if (args.length != 2) {
            System.err.println(errorMgs);
            System.err.println("ERROR: Invalid number of arguements.");
            System.exit(1);
        } else if (!args[0].equals("-f")) {
            System.err.println(errorMgs);
            System.err.println("ERROR: Must specify configuration file.");
            System.exit(1);
        } else {
            File f = new File(args[1]);
            if (!f.exists()) {
                System.err.println("The specified configuration file: " + args[1] + " is invalid");
                System.exit(1);
            }
        }
        return args[1];
    }
}
