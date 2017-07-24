package com.pagerank;

/**
 * Created by Naomi on 10/15/16.
 */
        import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.fs.FileSystem;
        import org.apache.hadoop.fs.Path;
        import org.apache.hadoop.io.*;
        import org.apache.hadoop.mapreduce.Job;
        import org.apache.hadoop.mapreduce.Mapper;
        import org.apache.hadoop.mapreduce.Reducer;
        import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
        import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
        import org.apache.hadoop.util.GenericOptionsParser;
        import java.io.IOException;
        import java.util.Arrays;
        import java.util.LinkedList;

public class WikiPageRank {
    public enum UpdateCounter {
        UPDATED, //Global counter for missing mass of dangling node
        numOfNodes, //Global counter to find number of nodes in the graph
        counter //counter to keep a count of Top 100 page ranks
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        conf.set("mapred.textoutputformat.separator", ":");
        if (otherArgs.length < 2) {
            System.err.println("Usage: PageRank <in> [<in>...] <out>");
            System.exit(2);
        }
        // Job to call the parser
        Job job0 = new Job(conf, "Parser call");
        job0.setJarByClass(WikiPageRank.class);
        job0.setMapperClass(Bz2WikiParser.ParsingMapper.class);
        job0.setReducerClass(Reducer.class);
        job0.setOutputKeyClass(Text.class);
        job0.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job0, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job0,
                new Path(otherArgs[1]+"graph"+0));
        boolean ok = job0.waitForCompletion(true);
        if (!ok) {
            throw new Exception("Job failed");
        }
        long n = job0.getCounters().findCounter(UpdateCounter.numOfNodes)
                .getValue();
        conf.setLong("numOfNodes", n);

        //10 Iterative jobs to calculate PageRank
        int ii = 0;
        while (ii < 10) {
            if (ii > 10) {
                throw new Exception("No path in 10 steps.");
            }
            Job job = new Job(conf, "page rank");
            job.setJarByClass(WikiPageRank.class);
            job.setMapperClass(PageRankMapper.class);
            job.setReducerClass(PageRankReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Node.class);
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path("graph" + (ii + 1)), true);
            FileInputFormat.addInputPath(job, new Path(otherArgs[1]+"graph"+ii));
            FileOutputFormat.setOutputPath(job,
                    new Path(otherArgs[1]+"graph"+(ii+1)));
            boolean complete = job.waitForCompletion(true);
            if (!complete) {
                throw new Exception("Job failed");
            }
            long counter = job.getCounters().findCounter(UpdateCounter.UPDATED)
                    .getValue();
            conf.setLong("delta",counter);
            fs.delete(new Path("graph" + ii), true);
            ++ii;
        }

        //Job to find Top 100 PageRanks
        Job job = new Job(conf, "Top100 rank");
        job.setJarByClass(WikiPageRank.class);
        job.setMapperClass(Top100Mapper.class);
        job.setPartitionerClass(RankPartitioner.class);
        job.setSortComparatorClass(RankComparator.class);
        job.setReducerClass(Top100Reducer.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]+"graph"+10));
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[1]+"graph"+11));
        boolean finish = job.waitForCompletion(true);
        if (!finish) {
            throw new Exception("Job failed");
        }
    }


    public static class PageRankMapper extends Mapper<Object,Text,Text,Node>{
        //Mapper emits Node with adjacency list and blank pagerank when emitting graph
        //Mapper emits Node with pagerank and blank adjacency list when emitting page rank contribution
        //Key is the Node ID
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            double pageRank = 0.0;
            //Calculate initial pagerank 1/num of nodes in graph
            double n = (1.0/Double.valueOf(conf.get("numOfNodes")));
            String[] line = value.toString().trim().split(":");
            String[] outlinks = null;
                String temp = line[1].trim();
                if(!line[1].equals("[]")){
                    outlinks = temp.substring(1, temp.length() - 1).trim().split(",");
                    context.write(new Text(line[0].trim()), new Node(new LinkedList<String>(Arrays.asList(outlinks))));
                }else
                    //Dangling node condition - create empty linked list
                    context.write(new Text(line[0].trim()), new Node(new LinkedList<String>()));
                if (line.length == 2) { //First time when page rank is 0.0
                    pageRank = n;
                } else {
                    //calculate page rank contribution
                    if (outlinks != null)
                        pageRank = Double.parseDouble(line[2]) / outlinks.length;
                    else
                    //Pick the existing page rank in case of dangling node
                        pageRank = Double.parseDouble(line[2]);
                }
            if(outlinks!=null){
                for (String val: outlinks) {
                    //Emit the adjanceny list
                    context.write(new Text(val.trim()), new Node(pageRank));
                }
            }else {
                //Update delta counter for dangling node missing mass
                context.getCounter(UpdateCounter.UPDATED).increment((long) (pageRank*n*1000000000));
            }

        }
    }


    // Reducer combines those records for same stationIDs which might be processed in different mappers so could
    // not be combined in one map task. After adding all those iterable values reducer calculates the average and emits
    // out the record for each stationID
    public static class PageRankReducer extends Reducer<Text,Node,Text,Node>{

        public void reduce (Text key, Iterable<Node> values, Context context)
                throws IOException, InterruptedException {
            Node m = new Node();
            double sum =0.0;
            LinkedList<Text> outlinks = new LinkedList<Text>();
            for (Node n: values) {
                if (!n.getOutlinks().isEmpty()){
                    //If it is a node copy the adjancency list
                    outlinks.addAll(n.getOutlinks());
                }
                else{
                    //Add the page rank contribution
                sum += n.getPagerank();
                }
            }
            double n = Double.valueOf(context.getConfiguration().get("numOfNodes"));
            if (context.getConfiguration().get("delta") != null)
                //Add delta node contribution
                sum += Double.valueOf(context.getConfiguration().get("delta"))/(1000000000);
            //Calculate page rank
            double pagerank = 0.15/n + (0.85*sum);
            m.setPagerank(pagerank);
            m.setOutlinks(outlinks);
            context.write(key,m);
        }
    }

}
