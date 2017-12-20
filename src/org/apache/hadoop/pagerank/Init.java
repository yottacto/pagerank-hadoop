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

public class Init {

    public static class InitMapper
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            StringTokenizer str = new StringTokenizer(value.toString());
            if (str.countTokens() == 0) return;
            
            String id = str.nextToken();
            String to = str.nextToken();
            if (!to.equals("0")) context.write(new Text(to), new Text("0"));
            if (to.equals("0")  || id.equals("0")) return;
            context.write(new Text(id), new Text(to));
        }
    }

    public static class InitReducer
            extends Reducer<Text, Text, Text, Text> {

        private long count;

        @Override
        protected void setup(Context context)
            throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            count = conf.getLong("count", 1);
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String str = "";
            double pr = 1. / count;
            for (Text val : values) {
                String tmp = val.toString();
                if (tmp.equals("0")) continue;
                str += " " + tmp;
            }

            String result = pr + str;
            context.write(key, new Text(result));
        }
    }

    public static void run(Map<String, String> path, long count)
            throws Exception {

        Configuration conf = new Configuration();
        conf.set("mapreduce.job.jar", "init.jar");
        conf.setLong("count", count);

        FileSystem fileSystem = FileSystem.get(URI.create(path.get("hdfs")), conf);
        String str = path.get("input");
        str = str.replaceAll("hdfs://.*:\\d+", "");

        Path fp = new Path(str);
        if (fileSystem.exists(fp)) {
            fileSystem.delete(fp, true);
            fileSystem.close();
        }

        Job job = Job.getInstance(conf, "page rank init");
        job.setJarByClass(Init.class);
        job.setMapperClass(InitMapper.class);
        // job.setCombinerClass(InitReducer.class);
        job.setReducerClass(InitReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(path.get("raw_input")));
        FileOutputFormat.setOutputPath(job, new Path(path.get("input")));

        job.waitForCompletion(true);
    }
}

