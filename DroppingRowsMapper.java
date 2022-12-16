import java.io.IOException;
import java.text.ParseException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class DroppingRowsMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    
    @Override
	public void map(LongWritable offset, Text rowText, Context context) throws IOException, InterruptedException 
    {
            // convert row to String
            String row = rowText.toString();
            
            // Split values by comma
            String[] fields = row.split(",", -1);

            for (String field:fields)
            {
                if (field.isEmpty())
                {
                    return;
                }
            }

            context.write(NullWritable.get(), new Text(rowText));  
       
    }
}
