
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class HadoopCensus {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, IntWritable> {

  
        public void map(LongWritable key, Text value, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            
            String[] attributes = line.split(", ");
            
            int age = Integer.parseInt(attributes[0]);
            int income = Integer.parseInt(attributes[2]);
            
            output.collect(new IntWritable(age),new IntWritable(income));
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        public void reduce(IntWritable key, Iterator<IntWritable> values, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
            
            int sum = 0;
            int counter = 0;
            while (values.hasNext()) {
                sum += values.next().get();
                counter++;
            }
            
            int avg = sum/counter;
            
            output.collect(key, new IntWritable(avg));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(HadoopCensus.class);
        conf.setJobName("HadoopCensus");

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

       // conf.setInputFormat(TextInputFormat.class);
       // conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
