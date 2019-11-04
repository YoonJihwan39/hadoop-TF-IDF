import java.io.IOException;
import java.util.StringTokenizer;

import java.lang.Math;
import java.io.BufferedReader; 
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import com.google.common.base.Charsets;

public class TFIDF {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      FileSplit fileSplit = (FileSplit)context.getInputSplit();
      String filename = fileSplit.getPath().getName();
      while (itr.hasMoreTokens()) {
	String token = itr.nextToken();
        word.set(token+ " " + filename);
        context.write(word, one);
      }
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  private static void CalcTFIDF(Path in, Path path, Configuration conf)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path file = new Path(path, "part-r-00000");

    ContentSummary cs = fs.getContentSummary(in);
    long D = cs.getFileCount();
    System.out.println("Total number of Document: " + D);

    if (!fs.exists(file))
      throw new IOException("Output not found!");

    BufferedReader br = null;
    BufferedReader temp = null;

    try {
      br = new BufferedReader(new InputStreamReader(fs.open(file), Charsets.UTF_8));
      temp = new BufferedReader(new InputStreamReader(fs.open(file), Charsets.UTF_8));
      int lines = 0;
      String line;

      while ((line = temp.readLine()) != null) lines++;
      temp.close();

      String str[] = new String[lines];
      int i = 0;
      while ((line = br.readLine()) != null) {
         str[i++] = line;
      }
      i = 0;
      while(i < lines){
         int nD = 1;
         String[] words = str[i].split("\\s");
         int j = i + 1;
         while(j < lines){
            String[] words2 = str[j].split("\\s");
            if(!words2[0].equals(words[0])){
               break;
            }
            nD++;
            j++;
         }
         int sum = Integer.parseInt(words[2]);
         double tf = Math.log(sum + 1);
         double tmp = (double)D / Math.min((nD+1), D);
         double idf = Math.log(tmp);
         double tfidf = tf * idf;
         System.out.println(words[0] + " from " + words[1] + " TF-IDF: " + tfidf);
         for(j = i + 1; j < nD + i; j++){
            String[] words2 = str[j].split("\\s");
            int sum2 = Integer.parseInt(words2[2]);
            double tf2 = Math.log(sum2 + 1);
            double tfidf2 = tf2 * idf;
            System.out.println(words2[0] + " from " + words2[1] + " TF-IDF: " + tfidf2);
         }
        i += nD;
      }

      return;
    } finally {
      if (br != null) {
        br.close();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(TFIDF.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    boolean result = job.waitForCompletion(true);
    Path outputpath = new Path(args[1]);
    Path inputpath = new Path(args[0]);
    CalcTFIDF(inputpath, outputpath, conf);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

