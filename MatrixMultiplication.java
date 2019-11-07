import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Parameter;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.ReflectionUtils;


public class MatrixMultiplication {

    // M矩阵映射函数
    public static class MatrixMapM extends Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // 划分i,j,value值
            String readLine = value.toString();
            String[] stringTokens = readLine.split(",");
			
			Text ovalue = new Text();
            IntWritable okey = new IntWritable(Integer.parseInt(stringTokens[0]));
			ovalue.set("0," + stringTokens[1] + "," + stringTokens[2]);
			context.write(okey, ovalue);
			
			// <int, (M, j, double)>
        }
    }
    // N矩阵映射
    public static class MatrixMapN extends Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // 划分i,j,value值
            String readLine = value.toString();
            String[] stringTokens = readLine.split(",");
			
			Text ovalue = new Text();
            IntWritable okey = new IntWritable(Integer.parseInt(stringTokens[1]));
			ovalue.set("1," + stringTokens[0] + "," + stringTokens[2]);
			context.write(okey, ovalue);
			
			// <int, (M, j, double)>
        }
    }
    // 合并MN
    public static class ReducerMN extends Reducer<IntWritable, Text, Text, DoubleWritable> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // M,N矩阵
            ArrayList<Text> M = new ArrayList<Text>();
            ArrayList<Text> N = new ArrayList<Text>();
            Configuration conf = context.getConfiguration();
            for(Text v : values) {
                Text tempV = ReflectionUtils.newInstance(Text.class, conf);
                ReflectionUtils.copy(conf, v, tempV);
				
                if (tempV.toString().charAt(0)  == '0') {
                    M.add(tempV);         // 添加到M集合
                } else {
                    N.add(tempV);         // 添加到N集合
                }
            }

            for(int i = 0; i < M.size(); i++) {
                for(int j = 0; j < N.size(); j++) {
					Text okey = new Text();
                    String[] tmpm = M.get(i).toString().split(",");
					String[] tmpn = N.get(j).toString().split(",");
					okey.set(tmpm[1] + "\t" + tmpn[1]);
					DoubleWritable ovalue = new DoubleWritable(Double.parseDouble(tmpm[2]) * Double.parseDouble(tmpn[2]));
                    context.write(okey, ovalue);
					// <(i,j), double>
                }
            }
        }
    }

    // map(i,j)需要做累加的值
    public static class MapMN extends Mapper<Object, Text, Text, DoubleWritable> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String readLine = value.toString();
            String[] pairValue = readLine.split("\t");
			Text okey = new Text();
			okey.set(pairValue[0] + "," + pairValue[1]);
            DoubleWritable ovalue = new DoubleWritable(Double.parseDouble(pairValue[2]));
            context.write(okey, ovalue);
        }
    }

    // reduce
    public static class ReduceMN extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            for(DoubleWritable value : values) {
                sum += value.get();
            }
            context.write(key, new DoubleWritable(sum));
        }
    }
	
    public static void main(String []args) throws Exception{
        // 乘法job
        Job jobinter = Job.getInstance();
        jobinter.setJobName("MapIntermediate");
        jobinter.setJarByClass(MatrixMultiplication.class);

        // 输入矩阵
        MultipleInputs.addInputPath(jobinter, new Path(args[0]), TextInputFormat.class, MatrixMapM.class);
        MultipleInputs.addInputPath(jobinter, new Path(args[1]), TextInputFormat.class, MatrixMapN.class);
        // 设置mapreduce
        jobinter.setReducerClass(ReducerMN.class);
		
        jobinter.setMapOutputKeyClass(IntWritable.class);
        jobinter.setMapOutputValueClass(Text.class);
        jobinter.setOutputKeyClass(Text.class);
        jobinter.setOutputValueClass(DoubleWritable.class);

        jobinter.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(jobinter, new Path(args[2]));

        jobinter.waitForCompletion(true);
       // 累加job
        Job job = Job.getInstance();
        job.setJobName("MapFinalOutput");
        job.setJarByClass(MatrixMultiplication.class);
        // 设置mapreduce
        job.setMapperClass(MapMN.class);
        job.setReducerClass(ReduceMN.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        
        job.waitForCompletion(true);
    }
}

