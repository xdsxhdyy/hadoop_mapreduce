package mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WorkCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private  LongWritable result = new LongWritable();
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        for (LongWritable v : values){
            count += v.get();
        }
        result.set(count);
        context.write(key, result);
    }
}
