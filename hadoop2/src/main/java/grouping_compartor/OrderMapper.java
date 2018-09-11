package grouping_compartor;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
    OrderBean order = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] info = value.toString().split("\t");
        order.setOrderid(Integer.parseInt(info[0]));
        order.setPrice(Double.parseDouble(info[2]));

        context.write(order, NullWritable.get());

    }
}
