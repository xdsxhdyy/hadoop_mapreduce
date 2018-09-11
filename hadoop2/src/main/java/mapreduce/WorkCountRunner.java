package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WorkCountRunner implements Tool {
    private  Configuration conf = null;


    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(conf, "LmWordCount");
        //将当前任务打成一个jar包并指定jar包运行的类
        job.setJarByClass(WorkCountRunner.class);
        //设置Mapper
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置reduce
        job.setReducerClass(WorkCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //为job设置数据的出入来源
        FileInputFormat.addInputPath(job, new Path("E:\\ws_upload\\words.txt"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\ws_upload\\output1"));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new WorkCountRunner(), args);
        System.exit(status);
    }
}
