import java.io.IOException;
import java.text.ParseException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CrimesFilterMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

  
    public String[] to_keep = {"ID", "Case Number", "Date", "Block", "IUCR", "Primary Type",
    "Description", "Location Description", "Arrest", "Domestic", "Beat",
    "District", "Ward", "Community Area", "FBI Code", "X Coordinate",
    "Y Coordinate", "Year", "Updated On", "Latitude", "Longitude",
    "Location"};
    public int[] idx_to_keep = {0,5,6,7,8,9,10,11,12,13,17,19,20};

    
    @Override
	public void map(LongWritable offset, Text rowText, Context context) throws IOException, InterruptedException 
    {
    //    ignore header field
        if (offset.get() != 0)
        {
            // convert row to String
            String row = rowText.toString();
            
            // Split values by comma
            String[] fields = row.split(",", -1);
            
            // output string
            String output = "";

            for (int i:idx_to_keep)
            {
                output+= fields[i].toString() + ",";
                
            }

            // removing extra comma
            output = output.substring(0, output.length() - 1);
            context.write(NullWritable.get(), new Text(output));  

        }
        else
        {
            // Updating headers
            
            String output = "";
            
            for (Integer i: idx_to_keep)
            {
                output = output + to_keep[i] + ",";
            }
            
            // removing extra comma
            output = output.substring(0, output.length() - 1);
            
            context.write(NullWritable.get(), new Text(output));

        }
        
    }
}
