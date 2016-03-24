package com.researchworx.genomics.gobjectingestion.plugincore;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalINIConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;

public class Config {

	private HierarchicalINIConfiguration iniConfObj;
	
	public Config(String configFile) throws ConfigurationException
	{
	    iniConfObj = new HierarchicalINIConfiguration(configFile);
	    iniConfObj.setAutoSave(true);
	}
	
	public String getParam(String paramName)
	{
		SubnodeConfiguration sObj = iniConfObj.getSection("general");
		return sObj.getString(paramName);
	}
	
	public String getParam(String sectionName, String paramName)
	{
		SubnodeConfiguration sObj = iniConfObj.getSection(sectionName);
		return sObj.getString(paramName);
	}
	
}