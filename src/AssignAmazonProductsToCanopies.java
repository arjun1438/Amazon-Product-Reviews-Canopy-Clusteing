package kmeans.amazon.clustering;

import java.io.IOException;
import java.util.StringTokenizer;
import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import kmeans.amazon.clustering.AmazonReviewCanopyCenterFinder.CanopyCenter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class AssignAmazonProductsToCanopies {
   

    public static class AmazonData{
    
        public String productID;
        public String productName; 
        public List<Double> userRatings = new ArrayList<Double>(); 
        public List<String> usersData = new ArrayList<String>();
        public List<Integer> canopiesData = new ArrayList<Integer>();
    
        public AmazonData(){
            productID =""; 
            productName = "";
        } 
  
        public AmazonData(String data){
            
            String amazonData[] = data.split("\t");
            
            productID = amazonData[0];  
            productName = "";
            String userRatingData[] = amazonData[1].split(";");
            for (String id : userRatingData) { 
                String tokens[] = id.split("->");
                usersData.add(tokens[0]);
                userRatings.add(Double.parseDouble(tokens[1]));
            }
        }


       public String getAmazonData()
       {
            return usersData.toString()+"\t"+userRatings.toString();
       }

       public String getCanopy()
       {
            return usersData.toString()+"\t"+userRatings.toString()+" "+canopiesData.toString();
       }
 } 
    public static class AssignAmazonProductsToCanopiesMapper extends Mapper<LongWritable, Text, Text, Text> 
    {   
        private Text productID = new Text();
        private Text dataOutput = new Text();
        private int  tooCloseMetric = 2;
        private List<CanopyCenter> canopyCentersList = new ArrayList<CanopyCenter>();
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           
                Configuration conf = new Configuration();
                SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file( new Path("/user/cloudc48/hw1/output/step2/sequenceFile")));
                Writable productKey = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                Writable userValue = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
                while (reader.next(productKey, userValue)) {
                    String syncSeen = reader.syncSeen() ? "*" : "";
                    CanopyCenter canopyCenter = new CanopyCenter(productKey.toString(), userValue.toString());
                    canopyCentersList.add(canopyCenter);
                }
                    
                IOUtils.closeStream(reader);

       AmazonData amazonData = new AmazonData(value.toString()); 
       
       for(int i =0; i < canopyCentersList.size(); i++)
        {
            ArrayList<String> reviewers = new ArrayList<String>(amazonData.usersData);
            List<String> data = canopyCentersList.get(i).userRatingsData;
            List<String> uData = new ArrayList<String>();
            for(String str : data)
            {
                String string[] = str.split("->");
                uData.add(string[0]);
            }

          reviewers.retainAll(uData);
          if(reviewers.size() >= 1)
          {
              amazonData.canopiesData.add(i);
          }
        }
        
        productID.set(amazonData.productID);
        dataOutput.set(amazonData.getCanopy());

        context.write(productID, dataOutput);
        }

    }

    public static class AssignAmazonProductsToCanopiesReducer extends Reducer<Text,Text,Text,Text> {
        private Text productID = new Text();
        private Text userData = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                userData.set(val);
                context.write(key, val);
            }
        } 
    }

    public static void main(String[] args) throws Exception {
        
        Configuration configuration = new Configuration();
        String[] arguments = new GenericOptionsParser(configuration, args).getRemainingArgs();
        if (arguments.length != 2) {
            System.err.println("Usage: Two paramters required <input dir> <output dir>");
            System.exit(2);
        }
        
        Job job = new Job(configuration, "Step3AssignAmazonProductsToCanopies");
        job.setJarByClass(AssignAmazonProductsToCanopies.class);
        job.setMapperClass(AssignAmazonProductsToCanopiesMapper.class);
        job.setCombinerClass(AssignAmazonProductsToCanopiesReducer.class);
        job.setReducerClass(AssignAmazonProductsToCanopiesReducer.class);
        job.setInputFormatClass (TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(arguments[0]));
        FileOutputFormat.setOutputPath(job, new Path(arguments[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

