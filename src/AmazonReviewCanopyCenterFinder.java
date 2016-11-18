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
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.JobConf;

public class AmazonReviewCanopyCenterFinder{

    public static class CanopyCenter{

    public Boolean isCenter;
    public String productID;
    public List<String> userRatingsData = new ArrayList<String>();
    public CanopyCenter(){
        isCenter = false;
        productID="";
    }

    public CanopyCenter(String data){
        Boolean isCenter = false;
        String Data[] = data.split("\t");
        productID = Data[0];
        String idsArray[] = Data[1].split(";");
        for (String id : idsArray) {
            String tokens[] = id.split("->");
            if(!tokens[0].equals("unknown"))
                userRatingsData.add(id);
        }
    }

    public CanopyCenter(String key, String data){
        Boolean isCenter = false;
        productID=key;
        data = data.replace(",","");
        data = data.replace("[","");
        data = data.replace("]","");
        String idsArray[] = data.split(" ");
        for (String userId : idsArray) {
            userRatingsData.add(userId);
        }
    }

    public void setCanopy(String key, String data){
        Boolean isCenter = false;
        productID=key;
        data = data.replace(",","");
        data = data.replace("[","");
        data = data.replace("]","");
        String idsArray[] = data.split(" ");
        for (String userId : idsArray) {
            userRatingsData.add(userId);
        }
    }

    public static int computerClosenessMetric(CanopyCenter centerItem, CanopyCenter centerCandidate, int threshold) {
         
         int count = 0;

         List<String> centerItemUserIDRating = centerItem.userRatingsData;
         String centerItemProductID = centerItem.productID;

         List<String> candidateUserIDRating = centerCandidate.userRatingsData;
         String candidateProductID = centerCandidate.productID;

        for(String item : centerItemUserIDRating)
        {
            String[] centerItemUserIDRatingSplit = item.split("->");
            for(String item1 : candidateUserIDRating){
               String[] candidateUserIDRatingSplit = item1.split("->");
               if((centerItemUserIDRatingSplit[0].equals(candidateUserIDRatingSplit[0])))
               {
                        count++;
                        
                        if(count >= threshold)
                            break;
               }
            }

        }

        return count;
    }
 
  }
   
    public static class AmazonReviewCanopyCenterFinderMapper extends Mapper<LongWritable, Text, Text, Text> {
    
        private Text productID = new Text();  
        private Text userRatingsData = new Text(); 
        private Text userIDsData = new Text(); 

        private List<CanopyCenter> canopyCentersList = new ArrayList<CanopyCenter>();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            CanopyCenter canopyCenterCandidate = new CanopyCenter(value.toString());
            if(canopyCentersList.isEmpty()){
                canopyCentersList.add(canopyCenterCandidate);
                productID.set(canopyCentersList.get(0).productID); 
                userRatingsData.set(canopyCentersList.get(0).userRatingsData.toString());
                context.write(productID, userRatingsData);
            }
            else{
                Boolean addToCanopyCenter = true;
                for (CanopyCenter canopyCenter : canopyCentersList) {         
                    if(CanopyCenter.computerClosenessMetric(canopyCenter, canopyCenterCandidate, 10) >= 10)
                    {
                         addToCanopyCenter = false;
                         break;
                    }
                }
                if(addToCanopyCenter==true){
                    canopyCentersList.add(canopyCenterCandidate);
                    productID.set(canopyCenterCandidate.productID); 
                    userRatingsData.set(canopyCenterCandidate.userRatingsData.toString());
                    context.write(productID, userRatingsData);
                }
            }
        }
    }       

    public static class AmazonReviewCanopyCenterFinderReducer extends Reducer<Text,Text,Text,Text> {
            
        private Text productID = new Text();  
        private Text userRatingsData = new Text();
        private int canopyClosenessThreshold = 2;
        private List<CanopyCenter> canopyCentersList = new ArrayList<CanopyCenter>();  
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            CanopyCenter canopyCenterCandidate = new CanopyCenter();
            for (Text val : values) {
                canopyCenterCandidate.setCanopy(key.toString(), val.toString());
                if(canopyCentersList.isEmpty()){
                    canopyCentersList.add(canopyCenterCandidate);
                    productID.set(canopyCentersList.get(0).productID); userRatingsData.set(canopyCentersList.get(0).userRatingsData.toString());
                    context.write(productID, userRatingsData);
                }
                else{
                    Boolean addToCanopyCenter = true;
                    for (CanopyCenter canopyCenter : canopyCentersList) {         
                        if(CanopyCenter.computerClosenessMetric(canopyCenter, canopyCenterCandidate, 10) >= 10)
                        {
                            addToCanopyCenter = false;
                            break;
                        }
 
                    }
                    if(addToCanopyCenter==true){
                        canopyCentersList.add(canopyCenterCandidate);
                        productID.set(canopyCenterCandidate.productID); userRatingsData.set(canopyCenterCandidate.userRatingsData.toString());
                        context.write(productID, userRatingsData);
                    }   
                }
            }
            
            Configuration configuration = new Configuration();
            FileSystem filesystem = FileSystem.get(configuration);
            Path sequenceFilePath = new Path("/user/cloudc48/hw1/output/step2/sequenceFile");
            SequenceFile.Writer fileWriter = SequenceFile.createWriter(configuration, Writer.file(sequenceFilePath), SequenceFile.Writer.compression(SequenceFile.CompressionType.RECORD, new GzipCodec()),Writer.keyClass(productID.getClass()), Writer.valueClass(userRatingsData.getClass()));
            boolean fopen = true;
            
            for (CanopyCenter canopyCenter : canopyCentersList) {
                productID.set(canopyCenter.productID); 
                userRatingsData.set(canopyCenter.userRatingsData.toString());
                fileWriter.append(productID, userRatingsData);
            }
            IOUtils.closeStream(fileWriter);

        }
    }

    public static void main(String[] args) throws Exception {
        
        Configuration configuration = new Configuration();
        String[] arguments = new GenericOptionsParser(configuration, args).getRemainingArgs();
        if (arguments.length != 2) {
            System.err.println("Two parameters required <input dir> <output dir>");
            System.exit(2);
        }
        
        Job job = new Job(configuration, "Step2CanopyCenterFinder");
        job.setJarByClass(AmazonReviewCanopyCenterFinder.class);
        job.setMapperClass(AmazonReviewCanopyCenterFinderMapper.class);
        job.setCombinerClass(AmazonReviewCanopyCenterFinderReducer.class);
        job.setReducerClass(AmazonReviewCanopyCenterFinderReducer.class);
        job.setInputFormatClass (TextInputFormat.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(arguments[0]));
        FileOutputFormat.setOutputPath(job, new Path(arguments[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    
    }
}


