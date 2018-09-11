package partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text,FlowBean> {
    FlowBean bean = new FlowBean();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] phone = line.split("\t");

        v.set(phone[1]);

        bean.setUpFlow(Long.parseLong(phone[phone.length - 3]));
        bean.setDownFlow(Long.parseLong(phone[phone.length - 2]));

        context.write(v,bean);
    }
}
