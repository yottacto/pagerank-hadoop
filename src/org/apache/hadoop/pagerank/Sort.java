package org.apache.hadoop.pagerank;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Sort {

    private static int count = 4;
    private static double damping = 0.85;

    public static class SortMapper
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            StringTokenizer str = new StringTokenizer(value.toString());
            if (str.countTokens() == 0)
                return;
            String id = str.nextToken();
            double pr = Double.parseDouble(str.nextToken());
            int count = str.countTokens();
            if (count == 0)
                count = 1;
            double average_pr = pr / count;
            String linkids = "$";
            while (str.hasMoreTokens()) {
                String linkid = str.nextToken();
                context.write(new Text(linkid), new Text("#" + average_pr));
                linkids += " " + linkid;
            }
            context.write(new Text(id), new Text(linkids));
        }
    }

    public static class SortReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String str = "";
            double pr = 0;
            for (Text val : values) {
                if (val.toString().substring(0, 1).equals("#")) {
                    pr += Double.parseDouble(val.toString().substring(1));
                } else if(val.toString().substring(0, 1).equals("$")) {
                    str += val.toString().substring(1);
                }
            }

            pr = damping * pr + (1 - damping) * 1. / count;
            String result = pr + str;
            context.write(key, new Text(result));
        }
    }

    public static void run(Map<String, String> path) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.job.jar", "sort.jar");

        FileSystem fileSystem = FileSystem.get(URI.create(path.get("hdfs")), conf);
        String str = path.get("result");
        str = str.replaceAll("hdfs://.*:\\d+", "");

        Path fp = new Path(str);
        if (fileSystem.exists(fp)) {
            fileSystem.delete(fp, true);
            fileSystem.close();
        }

        Job job = Job.getInstance(conf, "page rank");
        job.setJarByClass(Sort.class);
        job.setMapperClass(SortMapper.class);
        // job.setCombinerClass(SortReducer.class);
        job.setReducerClass(SortReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(path.get("output")));
        FileOutputFormat.setOutputPath(job, new Path(path.get("result")));

        job.waitForCompletion(true);
    }
}

