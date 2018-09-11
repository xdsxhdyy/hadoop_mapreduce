package outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream atguiguOutputStream = null;
    FSDataOutputStream otherOutputStream = null;

    public FilterRecordWriter(TaskAttemptContext job) throws IOException{
        //获取文件系统
        FileSystem fs = FileSystem.get(job.getConfiguration());
        //创建输出文件路径
        Path atguiguPath = new Path("E:\\ws_upload\\outputs\\atguigu.log");
        Path otherPath = new Path("E:\\ws_upload\\outputs\\other.log");

        //创建输出流
        atguiguOutputStream = fs.create(atguiguPath);
        otherOutputStream = fs.create(otherPath);
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        if(key.toString().contains("atguigu")){
            atguiguOutputStream.write(key.toString().getBytes());
        }else{
            otherOutputStream.write(key.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        if(atguiguOutputStream != null){
            atguiguOutputStream.close();
        }

        if(otherOutputStream != null){
            otherOutputStream.close();
        }
    }
}
