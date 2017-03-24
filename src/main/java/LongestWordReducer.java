import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LongestWordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private String longestWord = "";

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text(longestWord), new IntWritable(longestWord.length()));
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        if (key.toString().length() >= longestWord.length()) {
            longestWord = key.toString();
        }
    }
}
