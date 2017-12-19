package org.apache.hadoop.pagerank;

import java.io.IOException;
import java.net.URI;
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

public class Count {

    public static class CountMapper
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            StringTokenizer str = new StringTokenizer(value.toString());
            if (str.countTokens() == 0) return;
            while (str.hasMoreTokens()) {
                String linkid = str.nextToken();
                context.write(new Text(linkid), new Text(""));
            }
        }
    }

    public static class CountReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            context.getCounter("count", "count").increment(1);
        }
    }

    public static long run(Map<String, String> path) throws Exception {

        Configuration conf = new Configuration();
        conf.set("mapreduce.job.jar", "count.jar");

        FileSystem fileSystem = FileSystem.get(URI.create(path.get("hdfs")), conf);

        String str = path.get("tmp_output");
        str = str.replaceAll("hdfs://.*:\\d+", "");
        Path fp = new Path(str);
        if (fileSystem.exists(fp)) {
            fileSystem.delete(fp, true);
            fileSystem.close();
        }

        Job job = Job.getInstance(conf, "page rank count");
        job.setJarByClass(Count.class);
        job.setMapperClass(CountMapper.class);
        // job.setCombinerClass(CountReducer.class);
        job.setReducerClass(CountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(path.get("raw_input")));
        FileOutputFormat.setOutputPath(job, new Path(path.get("tmp_output")));
        job.waitForCompletion(true);

        // CounterGroup counters = job.getCounters().getGroup("count");
        // for (Counter counter:counters){
        //     System.out.println(counter.getDisplayName() + ":" + counter.getValue());
        // }

        return job.getCounters().findCounter("count", "count").getValue();
    }
}

