package comparator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 自定义Key，只要涉及到排序都是针对key
 */
public class FlowSortDriver extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf(), "FlowSortDriver");

        job.setJarByClass(FlowSortDriver.class);

        job.setMapperClass(FlowSortCountMapper.class);
        job.setMapOutputKeyClass(FlowSortBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(FlowSortCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowSortBean.class);

        job.setPartitionerClass(ProvinceFlowPartitioner.class);
        job.setNumReduceTasks(5);

        FileInputFormat.addInputPath(job, new Path("E:\\ws_upload\\phone_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\ws_upload\\output3"));
        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new FlowSortDriver(), args);
        System.exit(status);
    }
}
