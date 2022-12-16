import java.io.IOException;
import java.text.ParseException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DemoCountMissingMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public  String[] column_names = {"Community Area Number","COMMUNITY AREA NAME","PERCENT OF HOUSING CROWDED","PERCENT HOUSEHOLDS BELOW POVERTY","PERCENT AGED 16+ UNEMPLOYED","PERCENT AGED 25+ WITHOUT HIGH SCHOOL DIPLOMA","PERCENT AGED UNDER 18 OR OVER 64","PER CAPITA INCOME" ,"HARDSHIP INDEX"
};    
    @Override
	public void map(LongWritable offset, Text rowText, Context context) throws IOException, InterruptedException 
    {
       
        if (offset.get() != 0)
        {
            String row = rowText.toString();
        
            String[] fields = row.split(",", -1);
            int col = 0;
            for (String field: fields)
            {
		String s = column_names[col].toString();
                if (field.isEmpty())
                {
		   context.write(new Text(s), new IntWritable(1));
                }
                else
                {
                   context.write(new Text(s), new IntWritable(0));         
                }  
                col+=1;
            }

        }
        
    }
}
