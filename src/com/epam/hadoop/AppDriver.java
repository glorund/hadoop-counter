package com.epam.hadoop;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class AppDriver {
	
    public static void main( String[] args ) throws IOException
    {
	    long startTime  = System.currentTimeMillis();
    	Configuration conf = new Configuration();
	    
	    conf.set("fs.default.name", "hdfs://10.0.2.15");
	    
	    Map<String, Long> results = new HashMap<String, Long>();
	    FileSystem fileSystem = FileSystem.get(conf);
	    FileStatus[] files = fileSystem.listStatus(new Path("/local/ipin"));
	    for (FileStatus s: files) {
	        System.out.println((new StringBuilder("got file :")).append(s.getPath().getName()).toString());
            FSDataInputStream in = fileSystem.open(s.getPath());
            Parser parser = new Parser(new InputStreamReader(in));
            while(parser.hasNext()) {
                String key = parser.next();
                Long count = 1L;
                if (results.containsKey(key)) {
                    count += results.get(key);
                }
                results.put(key, count);
            }
            long stopTime = System.currentTimeMillis();
            System.out.println("spend "+ (stopTime - startTime) + " ms");
	    }

	    FSDataOutputStream out = fileSystem.create(new Path("/local/results"));
	    List<Entry<String,Long>> values = new ArrayList<Entry<String,Long>>(results.entrySet());
	    Collections.sort(values, new Comparator<Entry<String,Long>>() {
			public int compare(Entry<String, Long> a, Entry<String, Long> b) {
				Long diff = b.getValue() - a.getValue();
				return diff.intValue();
			}
		});
	    values = values.subList(0, 100);
	    Map <String, Long> actual  = new TreeMap<String, Long>();
	    for(Entry<String, Long> entry: values) {
	    	actual.put(entry.getKey(),entry.getValue());
	    }
	    for (String key : actual.keySet() ) {
	    	System.out.println(key + " - " + actual.get(key));
	    	out.writeUTF(key + " " + actual.get(key) + "\n");
	    }
	    // Get the Java runtime
	    long stopTime = System.currentTimeMillis();
		System.out.println("total time "+ ((stopTime - startTime)/1000) + " s");
	    Runtime runtime = Runtime.getRuntime();
	    // Calculate the used memory
	    long memory = runtime.totalMemory() - runtime.freeMemory();
	    System.out.println("Used memory is bytes: " + memory);
    }
}
