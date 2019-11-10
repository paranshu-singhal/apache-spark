package edu.ucr.cs.cs226.psing069;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class KNN 
{
    public static void main( String[] args ){

    try {
        Configuration conf = new Configuration();
        conf.set("QueryPoint",args[1]);
        conf.setInt("k", Integer.parseInt(args[2]));
        
        Job job = Job.getInstance(conf, "KNN");
        job.setJarByClass(KNN.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        job.waitForCompletion(true);
        
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch(IOException e){
            e.printStackTrace();
        }
    }

    public static class TokenizerMapper extends Mapper<Object, Text, DoubleWritable, Text>{
        
        private String querypoint;
        private DoubleWritable Distance = new DoubleWritable();
        private Text p = new Text();
        private String[] points;
        private Double point0, point1;
        
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            
            Configuration conf=context.getConfiguration();
            querypoint = conf.get("QueryPoint");
            points = querypoint.split(",");
             /* Take and split query point */
            point0 = Double.parseDouble(points[0]);
            point1 = Double.parseDouble(points[1]);
        }
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            Double dist;
            String str = value.toString();
            /* Split the string */
            String[] aftersplit = str.split(",");
             /* take the second and third values as points, since the first value is ID */
            double distancept1 = Double.parseDouble(aftersplit[1]);
            double distancept2 = Double.parseDouble(aftersplit[2]);
            /* Calculates the Euclidean distance */
            dist = (Math.sqrt(Math.pow((point0-distancept1),2) + Math.pow((point1-distancept2),2)));
             /* output (distance, point) as key value pair */
            //Distance.set(dist.longValue());
            Distance.set(dist);
            p.set(str);
            context.write(Distance, p);
        }
    }

    public static class IntSumReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
        
        private int count_v = 1;

        protected void setup(Context context) {
            count_v = 1;
        }
        public void reduce(DoubleWritable distance, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            
            Configuration conf = context.getConfiguration();
            int k = conf.getInt("k", 1);
            if (count_v <= k) {
                for (Text p : value) {
                    context.write(distance, p);
                    count_v = count_v + 1;
                }
            }
        }
    }
}
