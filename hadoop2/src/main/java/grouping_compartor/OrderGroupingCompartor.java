package grouping_compartor;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingCompartor extends WritableComparator {


    public OrderGroupingCompartor() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;
        int result;
        if(aBean.getOrderid() > bBean.getOrderid()){
            result = 1;
        }else if(aBean.getOrderid() < bBean.getOrderid()){
            result = -1;
        }else{
            result = 0;
        }
        return result;
    }
}
