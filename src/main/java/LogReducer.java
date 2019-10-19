import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LogReducer extends Reducer<Text, IntWritable,
                                Text, IntWritable> {

    int count = 0;

    /**
     * The datatype of key and values matches the datatype of the output of the Mapper
     * @param key key is the key of the mapper's output, in this case, the key is an IP
     * @param values each value in values is IntWritable, in this case, values is [1,1,1,1,1,...]
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Context context) throws IOException, InterruptedException {
        for(IntWritable value : values){
            count += value.get();
        }
        context.write(key, new IntWritable(count));
    }
}
