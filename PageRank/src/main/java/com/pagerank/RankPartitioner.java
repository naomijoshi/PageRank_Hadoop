package com.pagerank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by Naomi on 10/22/16.
 */
public class RankPartitioner extends Partitioner<DoubleWritable,Text> {


    @Override
    public int getPartition(DoubleWritable doubleWritable, Text text, int i) {
        //Assign all values to one reducer
        return 0;
    }
}
