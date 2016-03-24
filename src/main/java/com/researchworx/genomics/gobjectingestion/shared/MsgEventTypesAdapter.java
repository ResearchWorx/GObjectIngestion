package com.researchworx.genomics.gobjectingestion.shared;

import javax.xml.bind.annotation.adapters.XmlAdapter;
 
	public class MsgEventTypesAdapter extends XmlAdapter<String, MsgEventType> {

	    @Override
	    public MsgEventType unmarshal(String v) throws Exception {
			try {
				return Enum.valueOf(MsgEventType.class, v);
			} catch(IllegalArgumentException  e) {
				//ToDo: Add logging
			}
	        return null;
	    }

	    @Override
	    public String marshal(MsgEventType v) throws Exception {
	        return v.toString();
	    }

	}