package com.researchworx.genomics.gobjectingestion.plugincore;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalINIConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Config {
    private static final Logger logger = LoggerFactory.getLogger(Config.class);
    private HierarchicalINIConfiguration iniConfObj;

    Config(String configFile) throws ConfigurationException {
        logger.trace("Constructor called");
        logger.trace("Generating new [HierarchicalINIConfiguration from [configFile]");
        iniConfObj = new HierarchicalINIConfiguration(configFile);
        logger.trace("Enable [HierarchicalINIConfiguration].setAutoSave");
        iniConfObj.setAutoSave(true);
    }

    public String getParam(String paramName) {
        logger.trace("getParam(paramName) called");
        logger.trace("Grabbing [general] from [iniConfObj]");
        SubnodeConfiguration sObj = iniConfObj.getSection("general");
        logger.trace("Returning [{}] from [general]", paramName);
        return sObj.getString(paramName);
    }

    public String getParam(String sectionName, String paramName) {
        logger.trace("getParam(sectionName, paramName) called");
        logger.trace("Grabbing [{}] from [iniConfObj]", sectionName);
        SubnodeConfiguration sObj = iniConfObj.getSection(sectionName);
        logger.trace("Returning [{}] from [{}]", paramName, sectionName);
        return sObj.getString(paramName);
    }

}