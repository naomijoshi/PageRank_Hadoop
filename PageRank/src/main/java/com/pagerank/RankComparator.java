package com.pagerank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by Naomi on 10/22/16.
 */
public class RankComparator extends WritableComparator {

    protected RankComparator() {
        super(DoubleWritable.class, true);
    }

    public int compare(WritableComparable w1, WritableComparable w2) {
        DoubleWritable key1 = (DoubleWritable) w1;
        DoubleWritable key2 = (DoubleWritable) w2;
        //Sort in descending order
        int result =  Double.compare(key2.get(),key1.get());
        return result;
    }
}
