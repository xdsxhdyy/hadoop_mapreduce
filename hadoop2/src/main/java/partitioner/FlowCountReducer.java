package partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    FlowBean resultBean = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long upflow = 0;
        long downflow = 0;
        for (FlowBean flow : values){
            upflow += flow.getUpFlow();
            downflow += flow.getDownFlow();
        }
        resultBean.setUpFlow(upflow);
        resultBean.setDownFlow(downflow);
        resultBean.setSumFlow(upflow + downflow);

        context.write(key,resultBean);
    }
}
