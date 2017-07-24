package com.pagerank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * Created by Naomi on 10/22/16.
 */
public class Top100Reducer extends Reducer<DoubleWritable,Text,Text,DoubleWritable> {

    public void reduce (DoubleWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        long count = context.getCounter(WikiPageRank.UpdateCounter.counter).getValue();
        if(count < 100){
            for (Text val:values) {
                context.getCounter(WikiPageRank.UpdateCounter.counter).increment(1);
                context.write(val,key);
            }
        }

    }
}
