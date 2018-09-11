package comparator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowSortCountMapper extends Mapper<LongWritable, Text, FlowSortBean, Text> {

    Text phoneno = new Text();
    FlowSortBean flow = new FlowSortBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\t");

        String phone = words[1];
        Long upflow = Long.valueOf(words[words.length - 3]);
        Long downflow = Long.valueOf(words[words.length - 2]);

        phoneno.set(phone);
        flow.setUpFlow(upflow);
        flow.setDownFlow(downflow);

        context.write(flow,phoneno);
    }
}
