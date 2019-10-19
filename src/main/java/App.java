import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Driver class (main method)
 * Create a job in the main class and set the attributes of the job
 */
public class App {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config, "IP Frequency");

        job.setJarByClass(App.class);

        /**Set mapper and reducer classes*/
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        /**Set InputFormat and OutputFormat classes*/
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        /**Set the output key and value type*/
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        /**Set input and output path*/
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true)? 0 : 1);
    }
}
