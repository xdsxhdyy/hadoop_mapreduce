package grouping_compartor;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义key必须实现排序，WritableComparable。分区里的数据必须是有序的
 */
public class OrderBean implements WritableComparable<OrderBean> {
    private int orderid;
    private double price;

    public OrderBean() {
        super();
    }


    public OrderBean(int orderid, double price) {
        super();
        this.orderid = orderid;
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean o) {
        int result;
        if(orderid > o.getOrderid()){
            result = 1;
        }else if(orderid < o.getOrderid()){
            result = -1;
        }else{
            result = price > o.getPrice() ? -1 : 1;
        }
        /*if(orderid == o.getOrderid()){
            result = price > o.getPrice() ? -1 : 1;
        }*/

        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(orderid);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderid = dataInput.readInt();
        this.price = dataInput.readDouble();
    }

    public int getOrderid() {
        return orderid;
    }

    public void setOrderid(int orderid) {
        this.orderid = orderid;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return orderid + "\t" + price;
    }
}
