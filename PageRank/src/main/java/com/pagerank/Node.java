package com.pagerank;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;

/**
 * Created by Naomi on 10/17/16.
 */
public class Node implements Writable {
    private LinkedList<Text> outlinks = new LinkedList<Text>(); //adjacency list
    private double pagerank; //pagerank value

    public Node() {
        pagerank = Double.MIN_VALUE;
    }

    public Node(LinkedList<String> outl, double pr) {
        for (String node : outl) {
            Text temp = new Text(node.trim());
            outlinks.add(temp);
        }
        pagerank = pr;
    }


    public Node(LinkedList<String> outl) {
        for (String node : outl) {
            Text temp = new Text(node.trim());
            outlinks.add(temp);
        }
    }

    public Node(double pr) {
        pagerank = pr;
    }


    public LinkedList<Text> getOutlinks() {
        return outlinks;
    }

    public void setOutlinks(LinkedList<Text> outlinks) {
        this.outlinks = outlinks;
    }

    public double getPagerank() {
        return pagerank;
    }

    public void setPagerank(double pagerank) {
        this.pagerank = pagerank;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        IntWritable size = new IntWritable(this.getOutlinks().size());
        size.write(dataOutput);
        for (Text outl : outlinks) {
            outl.write(dataOutput);
        }
            dataOutput.writeDouble(pagerank);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
            outlinks.clear();
            IntWritable size = new IntWritable();
            size.readFields(dataInput);
            int n = size.get();
            while (n-- > 0) {
                Text outlink = new Text();
                outlink.readFields(dataInput);
                    outlinks.add(outlink);
        }
        pagerank = dataInput.readDouble();
    }
        @Override
        public String toString () {
            return (outlinks.toString() + ":" +pagerank);
        }
    }
