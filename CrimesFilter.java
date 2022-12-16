import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CrimesFilter
{
 public static void main(String[] args) throws Exception
 {
    if (args.length != 2) {
        System.err.println("Usage: CrimesFilter <input path> <output path>");
        System.exit(-1);
      }
      
      Job job = Job.getInstance();
      job.setJarByClass(CrimesFilter.class);
      job.setJobName("Crimes Filter");
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      job.setMapperClass(CrimesFilterMapper.class);
      job.setNumReduceTasks(0);
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(Text.class);
      System.exit(job.waitForCompletion(true) ? 0 : 1);
 
 }
}
