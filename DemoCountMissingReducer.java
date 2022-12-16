import java.io.IOException;
import java.util.List;
import java.lang.Double;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class DemoCountMissingReducer
    extends Reducer<Text, IntWritable, Text,  Text> {
  
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
      
        int total;
	total=0;
        for(IntWritable value : values)
        {
            total += value.get();
           
        }
        String v =  String.valueOf(total);
     
    
        context.write(new Text(key), new Text(v));
  }
    
}
