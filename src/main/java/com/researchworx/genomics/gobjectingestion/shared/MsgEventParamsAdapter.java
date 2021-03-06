package com.researchworx.genomics.gobjectingestion.shared;

import java.util.*;
import javax.xml.bind.annotation.adapters.XmlAdapter;

class MsgEventParamsAdapter extends XmlAdapter<MsgEventParamsAdapter.AdaptedMap, Map<String, String>> {
    static class AdaptedMap {
        List<Entry> entry = new ArrayList<>();
    }

    private static class Entry {
        String key;
        String value;
    }

    @Override
    public Map<String, String> unmarshal(AdaptedMap adaptedMap) throws Exception {
        Map<String, String> map = new HashMap<>();
        for (Entry entry : adaptedMap.entry) {
            map.put(entry.key, entry.value);
        }
        return map;
    }

    @Override
    public AdaptedMap marshal(Map<String, String> map) throws Exception {
        AdaptedMap adaptedMap = new AdaptedMap();
        for (Map.Entry<String, String> mapEntry : map.entrySet()) {
            Entry entry = new Entry();
            entry.key = mapEntry.getKey();
            entry.value = mapEntry.getValue();
            adaptedMap.entry.add(entry);
        }
        return adaptedMap;
    }

}