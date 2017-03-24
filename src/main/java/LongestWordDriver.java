import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LongestWordDriver extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Path output = new Path(args[1]);
        Job job = Job.getInstance(getConf());
        job.setJobName("Get the longest word app");
        job.setJarByClass(LongestWordDriver.class);
        job.setMapperClass(LongestWordMapper.class);
        job.setReducerClass(LongestWordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, output);
        output.getFileSystem(getConf()).delete(output, true);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int returnCode = ToolRunner.run(new Configuration(), new LongestWordDriver(), args);
        System.exit(returnCode);
    }
}