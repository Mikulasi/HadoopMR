import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LongestWordTest {
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

    @Before
    public void setUp() {
        LongestWordMapper mapper = new LongestWordMapper();
        LongestWordReducer reducer = new LongestWordReducer();
        mapDriver = new MapDriver<>();
        mapDriver.setMapper(mapper);
        reduceDriver = new ReduceDriver<>();
        reduceDriver.setReducer(reducer);
        mapReduceDriver = new MapReduceDriver<>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);

    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("maper mapper reducer"));
        mapDriver.withOutput(new Text("maper"), new IntWritable(5));
        mapDriver.withOutput(new Text("mapper"), new IntWritable(6));
        mapDriver.withOutput(new Text("reducer"), new IntWritable(7));
        mapDriver.runTest();

    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(5));
        values.add(new IntWritable(6));
        values.add(new IntWritable(7));
        reduceDriver.withInput(new Text("maper"), values);
        reduceDriver.withInput(new Text("mapper"), values);
        reduceDriver.withInput(new Text("reducer"), values);
        reduceDriver.withOutput(new Text("reducer"), values.get(2));
        reduceDriver.runTest();

    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text("maper mapper reducer"));
        mapReduceDriver.withOutput(new Text("reducer"), new IntWritable(7));
        mapReduceDriver.runTest();

    }
}