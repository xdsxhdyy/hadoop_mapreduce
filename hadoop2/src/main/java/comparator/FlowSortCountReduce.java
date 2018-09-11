package comparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowSortCountReduce extends Reducer<FlowSortBean, Text, Text, FlowSortBean> {
    @Override
    protected void reduce(FlowSortBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text text : values){
            context.write(text, key);
        }

        /*long upf =0;
        long downf = 0;
        Text value = null;
        for(Text text : values){

            upf += key.getUpFlow();
            downf += key.getDownFlow();
            value = text;
        }
        key.setUpFlow(upf);
        key.setDownFlow(downf);
        key.setSumFlow(upf + downf);

        context.write(value,key);*/
    }
}
