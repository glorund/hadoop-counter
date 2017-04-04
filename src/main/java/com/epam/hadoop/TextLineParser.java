package com.epam.hadoop;

public class TextLineParser {

    public String parseLogLine(String line) {
        String fields[] = line.split("\\s+");
        if (fields.length < 3) {
            return "";//empty object
        }
        return String.valueOf(fields[2]);
    }
}
