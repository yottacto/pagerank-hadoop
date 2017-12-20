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

public class PageRank {

    private static int count = 4;
    private static double damping = 0.85;

    public static class PageRankMapper
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

    public static class PageRankReducer
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

    public static void main(String[] args) throws Exception {
        Map<String, String> path = init(args);
        long n = Count.run(path);
        // long n = 284303; // for small data
        // long n = 870346; // for big   data
        System.out.println("=====================================================");
        System.out.println("n = " + n);
        System.out.println("=====================================================");
        Init.run(path, n);
        int iteration = 1;
        String output = path.get("output");
        for (int i = 0; i < iteration; i++) {
            path.replace("output", output + i);
            PageRank.run(path);
            path.replace("input", path.get("output"));
        }
        
        if (iteration == 0)
        	path.replace("output", path.get("input"));
        else
        	path.replace("output", output + (iteration - 1));
        Sort.run(path);
    }

    public static void run(Map<String, String> path) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.job.jar", "pagerank.jar");

        FileSystem fileSystem = FileSystem.get(URI.create(path.get("hdfs")), conf);
        String str = path.get("output");
        str = str.replaceAll("hdfs://.*:\\d+", "");

        Path fp = new Path(str);
        if (fileSystem.exists(fp)) {
            fileSystem.delete(fp, true);
            fileSystem.close();
        }

        Job job = Job.getInstance(conf, "page rank");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PageRankMapper.class);
        // job.setCombinerClass(PageRankReducer.class);
        job.setReducerClass(PageRankReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(path.get("input")));
        FileOutputFormat.setOutputPath(job, new Path(path.get("output")));

        job.waitForCompletion(true);
    }

    private static Map<String, String> init(String[] args) throws IOException {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage: page rank <hdfs> <in> <out>");
            System.exit(2);
        }

        String hdfs = otherArgs[0];

        Map<String, String> path = new HashMap<String, String>();
        path.put("hdfs", hdfs);
        path.put("raw_input", hdfs + "/pagerank/raw_input");
        path.put("input", hdfs + "/pagerank/input");
        path.put("tmp_output", hdfs + "/pagerank/output/tmp");
        path.put("output", hdfs + "/pagerank/output");
        path.put("result", hdfs + "/pagerank/result");
        return path;
    }
}

