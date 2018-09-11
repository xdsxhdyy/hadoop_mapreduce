package grouping_compartor;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class OrderDriver extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf(), "OrderGroup");

        job.setJarByClass(OrderDriver.class);

        job.setMapperClass(OrderMapper.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(OrderReducer.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setPartitionerClass(OrderPartitioner.class);
        job.setNumReduceTasks(3);

        job.setGroupingComparatorClass(OrderGroupingCompartor.class);

        FileInputFormat.addInputPath(job, new Path("E:\\ws_upload\\order_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\ws_upload\\output6"));

        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new OrderDriver(), args);
        System.exit(status);
    }

}
