package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * knn算法实现Testrecord's Y value
 * 调用参数
 * args[0]:训练数据路径
 * args[1]:测试数据路径
 * args[2]:OutPut Path
 * args[3]:k
 * 
 * 由大量的历史数据(训练集)预测新的数据(测试集)。
 * 训练集一般是比较大，而测试集比较小，所以进入map的是训练集。而把测试集放入内存。
 * 
 * @author Raymond
 * @date 2016-08-12
 */
public class MyKnnDriver extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		int res =0;
		try
        {
			res = ToolRunner.run(new Configuration(), new MyKnnDriver(), args);
        }
		catch(Exception e){
			e.printStackTrace();
		}
        System.exit(res);
	}

    public int run(String[] args) throws Exception {
    	//解析参数 训练数据集Path
    	String trainDataPath = args[0];
    	//解析参数 测试数据路径
    	String testFilePath = args[1];
    	//解析参数 输出结果路径
    	String outPutFilePath = args[2];
    	//最近邻算法-K参数
    	int K = Integer.parseInt(args[3]);
    	
        Configuration conf = getConf();
        conf.setInt("K", K);
        conf.set("TestFilePath", testFilePath);

		Job job = Job.getInstance(conf,"My Knn Model Job01");
        job.setJarByClass(MyKnnDriver.class);
        job.setMapperClass(MyKnnMapper.class);
        job.setCombinerClass(MyKnnCombiner.class);
        job.setReducerClass(MyKnnReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(MyTestRecord.class);
        
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        Path out =new Path(outPutFilePath);
        out.getFileSystem(conf).delete(out, true);
        
        FileInputFormat.addInputPath(job, new Path(trainDataPath));
        FileOutputFormat.setOutputPath(job, out);
        int res = job.waitForCompletion(true) ? 0 : 1;
        return res;
    }
}
