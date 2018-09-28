package com.es.pc.esexample.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public class FilingUtils {

    public static Map<String, Object> fromJSON(String jsonString) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(jsonString, Map.class);
    }

    public static String toJSON(Object value) throws JsonProcessingException {
        ObjectMapper obj = new ObjectMapper();
        return obj.writer().writeValueAsString(value);
    }
}
