package com.epam.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;

public class Parser implements Iterator<String>{
    private BufferedReader reader;
    private String token;

    public Parser(Reader fileReader) {
        reader = new BufferedReader(fileReader);
    }

	public boolean hasNext() {
        try {
            String nextLine = reader.readLine();
            if (nextLine == null) {
                return false;
            }
            String fields[] = nextLine.split("\\s+");
            if (fields.length < 3 || StringUtils.isEmpty(fields[2]) || "null".equals(fields[2])) {
                return hasNext();
            }
            token= String.valueOf(fields[2]);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public String next() {
        return token;
    }
    public void remove() {
    	throw new UnsupportedOperationException();
	}
}
