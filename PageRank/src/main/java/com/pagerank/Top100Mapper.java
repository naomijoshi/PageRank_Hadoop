package com.pagerank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Naomi on 10/22/16.
 */
public class Top100Mapper extends Mapper<Object,Text,DoubleWritable,Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split(":");
        context.write(new DoubleWritable(Double.parseDouble(line[2])),new Text(line[0]));
    }
}
