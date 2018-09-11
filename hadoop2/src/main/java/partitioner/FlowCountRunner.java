package partitioner;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 自定义value实现：按照手机号码聚合该月的所有流量，并输出
 */
public class FlowCountRunner extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf(),"自定义value-FlowCountClass");
        job.setJarByClass(FlowCountRunner.class);

        job.setMapperClass(FlowCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setReducerClass(FlowCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //设置分区规则，及reducetask的数量
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(5);

        FileInputFormat.addInputPath(job, new Path("E:\\ws_upload\\phone_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\ws_upload\\output1"));

        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new FlowCountRunner(), args);
        System.exit(status);
    }
}
