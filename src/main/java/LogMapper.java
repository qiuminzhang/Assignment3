import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    IntWritable one = new IntWritable(1);
    Text text = new Text();
    /**
     *
     * @param key position of the line in the file
     * @param value a line of text
     * @param context use context.write() to emit
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String IP = line.split(" ")[0];

        text.set(IP);
        context.write(text, one);
    }
}
