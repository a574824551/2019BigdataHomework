/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Modified by Shimin Chen to demonstrate functionality for Homework 2
// April-May 2019

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Hw2Part1{

  // This is the Mapper class
  // reference: http://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/mapreduce/Mapper.html
  //
  public static class SourceMapper extends Mapper<Object, Text, Text, Text>{

    private Text source_destination = new Text();
    private Text count_time = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	
		String[] line = value.toString().split("\\s+");//split /t, /n and space
		if (line.length == 3 ) {//Make sure the number of parameters is correct
			source_destination.set(line[0]+" "+line[1]);//key 
			count_time.set("1" + " " + line[2]);//value, "1" is used to count the number.
			context.write(source_destination, count_time);
		}
    }
  }
  

  public static class SumReducer extends Reducer<Text,Text,Text,Text> {

    private Text result_time= new Text();
	
    private byte[] prefix;
    private byte[] suffix;

    protected void setup(Context context) {
      try {
        prefix= Text.encode("average of ").array();
        suffix= Text.encode(" =").array();
      } catch (Exception e) {
        prefix = suffix = new byte[0];
      }
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      
	  int count_test = 0;
	  int count_sum = 0;
	  double time_sum = 0;
	  String[] temp;
      for (Text val : values) {
		  count_test++;
	      temp = val.toString().split("\\s+");
		  try{ //Ensure that the data conforms to the format.
			count_sum = count_sum + Integer.parseInt(temp[0]);
			time_sum = time_sum + Double.parseDouble(temp[1]);
		  }catch (Exception e){
			e.printStackTrace();
          }
	  }
	  System.out.println(count_test);
	  System.out.println(count_sum);
	  double avg_time = time_sum/count_sum;//calculate average time.
      result_time.set(count_sum + " " + String.format("%.3f",avg_time));//Keep three decimal places, and formatted output.
      context.write(key, result_time);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: Hw2Part1 <in> [<in>...] <out>");
      System.exit(2);
    }

    Job job = Job.getInstance(conf, "Hw2Part1");

    job.setJarByClass(Hw2Part1.class);

    job.setMapperClass(SourceMapper.class);
    job.setReducerClass(SumReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // add the input paths as given by command line
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }

    // add the output path as given by the command line
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
