
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

/*
     * Map class is static and extends MapReduceBase and implements Mapper 
     * interface having four hadoop generics type LongWritable, Text, Text, Text.
     */
  
    public  class PatentMapper extends Mapper<LongWritable, Text, Text, Text> {
         
        //Mapper
         
         
        /*
         *This method takes the input as text data type and and tokenizes input
         * by taking whitespace as delimiter. Now key value pair is made and this key 
         * value pair is passed to reducer.                                             
         * @method_arguments key, value, output, reporter
         * @return void
         */
         
        //Defining a local variable K of type Text
        Text k= new Text();
 
        //Defining a local variable v of type Text 
        Text v= new Text(); 
 
   
         
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
 
 
            //Converting the record (single line) to String and storing it in a String variable line
            String line = value.toString(); 
 
            //StringTokenizer is breaking the record (line) according to the delimiter whitespace
            StringTokenizer tokenizer = new StringTokenizer(line," "); 
  
            //Iterating through all the tokens and forming the key value pair   
 
            while (tokenizer.hasMoreTokens()) { 
 
                /* 
                 * The first token is going in jiten, second token in jiten1, third token in jiten,
                 * fourth token in jiten1 and so on.
                 */
 
                String jiten= tokenizer.nextToken();
                k.set(jiten);
                String jiten1= tokenizer.nextToken();
                v.set(jiten1);
 
                //Sending to output collector which inturn passes the same to reducer
                context.write(k,v); 
            } 
        } 
    } 
     