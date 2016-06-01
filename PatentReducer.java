
import java.io.IOException;
import java.util.StringTokenizer; 
  
/*
 * All org.apache.hadoop packages can be imported using the jar present in lib 
 * directory of this java project.
 */
 
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class PatentReducer extends Reducer<Text, Text, Text, IntWritable> {
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {

        //Defining a local variable sum of type int

        int sum = 0; 

        /*
         * Iterates through all the values available with a key and add them together 
         * and give the final result as the key and sum of its values
         */

        for(Text x : values)
        {
            sum++;
        }
         
        //Dumping the output in context object
         
        context.write(key, new IntWritable(sum)); 
    } 

} 
 