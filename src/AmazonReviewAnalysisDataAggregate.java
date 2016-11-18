package kmeans.amazon.clustering;

import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;

public class AmazonReviewAnalysisDataAggregate
{
    public static class AmazonReviewAnalysisDataAggregateMapper
            extends Mapper < LongWritable, Text, Text, Text>
        {
            private Text productID  = new Text ();
            private Text userRatingsData = new Text ();
	        private int lineNumber = 0;
	         String record = "";

            String fileName = new String();
            protected void setup(Context context) throws java.io.IOException, java.lang.InterruptedException
            {
                Path fileNameString = ((FileSplit) context.getInputSplit()).getPath();
                fileName = fileNameString.getName();
            }
		
	        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		        switch(lineNumber)
                {
                    case 0: String[] lineOne = value.toString().split(" ");
                            productID.set(lineOne[1]);
                            lineNumber += 1;
                            break;

                    case 3: String[] lineFour = value.toString().split(" ");
                            record += lineFour[1]+"->";
                            lineNumber+=1;
                            break;

                    case 6: String[] lineSeven = value.toString().split(" ");
                            record += lineSeven[1]+";";
                            lineNumber += 1;
                            break;
                        
                   case 10: userRatingsData.set(record);
                            context.write(productID, userRatingsData);
                            record=""; 
                            lineNumber = 0;
                            break;
                        
                  default: lineNumber+=1;
               }
	       }
      }

    public static class AmazonReviewAnalysisDataAggregateReducer
            extends Reducer < Text, Text, Text, Text >
        {
            private Text output = new Text ();

	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                  String tokens = "";
                        for (Text val : values) {
                                tokens += val.toString();
                        }
                        output.set(tokens);
                        context.write(key, output);
                }

        }

    public static void main (String[]args) throws Exception
    {
        if (args.length != 2) 
        {
            System.out.println("Two parameters are required- <input dir> <output dir>");
	    System.exit(417);
        }
 
        Configuration configuration = new Configuration ();
	    String[] arguments = new GenericOptionsParser(configuration, args).getRemainingArgs();
        Job job = new Job (configuration, "Step1AmazonReviewDataAggregation");
        job.setJarByClass (AmazonReviewAnalysisDataAggregate.class);

        job.setMapperClass (AmazonReviewAnalysisDataAggregateMapper.class);
        job.setCombinerClass (AmazonReviewAnalysisDataAggregateReducer.class);
        job.setReducerClass (AmazonReviewAnalysisDataAggregateReducer.class);
	
	    job.setInputFormatClass (TextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass (Text.class);
        job.setOutputValueClass (Text.class);

        FileInputFormat.addInputPath(job, new Path(arguments[0]));
        FileOutputFormat.setOutputPath(job, new Path(arguments[1]));
 
        job.setNumReduceTasks(1);
        
        System.exit (job.waitForCompletion (true) ? 0 : 1);
    }
}
