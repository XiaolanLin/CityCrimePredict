package analysis.count;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author: yummin
 * Date: 15/11/18
 */
public class YearAttributePair implements Writable, WritableComparable<YearAttributePair> {

    public IntWritable year;
    public Text attribute;


    public YearAttributePair(int year, String attr) {
        this.year = new IntWritable(year);
        this.attribute = new Text(attr);
    }

    public YearAttributePair(IntWritable year, Text attr) {
        this.year = year;
        this.attribute = attr;
    }

    public YearAttributePair() {
        this.year = new IntWritable();
        this.attribute = new Text();
    }

    public void write(DataOutput dataOutput) throws IOException {
        year.write(dataOutput);
        attribute.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        year.readFields(dataInput);
        attribute.readFields(dataInput);
    }

    public int compareTo(YearAttributePair o) {

        int result = this.year.compareTo(o.year);

        if (result != 0)
            return result;

        if (this.attribute.toString().equals("*"))
            return -1;
        else if (o.attribute.toString().equals("*"))
            return 1;

        return this.attribute.compareTo(o.attribute);
    }

    public boolean equals(Object o) {

        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        YearAttributePair pair = (YearAttributePair) o;

        if (attribute != null ? !attribute.equals(pair.attribute) : pair.attribute != null)
            return false;

        if (year != null ? !year.equals(pair.year) : pair.year != null)
            return false;

        return true;
    }

    public String toString() {
        return "(" + year.get() + ", " + attribute.toString() + ")";
    }
}
