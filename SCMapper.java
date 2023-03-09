package com.sc;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class SCMapper extends Mapper<Object, Text, Text,Text> {
//	private Text context = new Text();
	private String searchContext;
	
//	public void setup(Context context) throws IOException, InterruptedException{
//		Configuration conf = context.getConfiguration();
//		searchContext = conf.get("context");
//		
//	}
	 
	public void map (Object key, Text value, Context context) throws IOException, InterruptedException{
		
		Configuration conf = context.getConfiguration();
		searchContext = conf.get("context");
		String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
		
		StringTokenizer tokenizer = new StringTokenizer(value.toString());
		
		while(tokenizer.hasMoreTokens()){
			String word = tokenizer.nextToken();
			
			if(word.equals(searchContext)){
				context.write(new Text(filename), value);
				break;
			}
		}
		
		
	}

}
