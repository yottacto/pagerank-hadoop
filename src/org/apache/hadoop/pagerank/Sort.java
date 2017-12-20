package org.apache.hadoop.pagerank;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Sort {

    public static class SortMapper
            extends Mapper<Object, Text, DoubleWritable, Text> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            StringTokenizer str = new StringTokenizer(value.toString());
            if (str.countTokens() == 0)
                return;
            String id = str.nextToken();
            DoubleWritable pr = new DoubleWritable(Double.parseDouble(str.nextToken()));
            context.write(pr, new Text(id));
        }
    }

    public static class SortReducer
            extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {

        public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text val : values) {
                String str = val.toString();
                context.write(key, new Text(str));
            }
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

        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        
        job.setSortComparatorClass(SortDoubleComparator.class);

        FileInputFormat.addInputPath(job, new Path(path.get("output")));
        FileOutputFormat.setOutputPath(job, new Path(path.get("result")));

        job.waitForCompletion(true);
    }
}

