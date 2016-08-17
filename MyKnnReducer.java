package test;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Raymond
 * @date 2016-8-03
 * */
public class MyKnnReducer extends Reducer<IntWritable, MyTestRecord, NullWritable, Text> {

	private Text value = new Text();
	static Logger log = LoggerFactory.getLogger(MyKnnReducer.class);
	@Override
	public void reduce(IntWritable key, Iterable<MyTestRecord> values,Context cxt) 
			throws IOException, InterruptedException{
		
		//合并相同测试记录的最邻近训练记录， 得到最终的最邻近列表。从而得到测试记录的预测类别
		MyTestRecord resultTestRecord = null;
		for(MyTestRecord t:values){
			if(resultTestRecord ==null)
				resultTestRecord = t;
			else{
				resultTestRecord.merge(t);
			}
		}
		//按要求格式输出
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < resultTestRecord.getAttributes().length; i++) {
			sb.append(resultTestRecord.getAttributes()[i]).append(",");
		}
		sb.append(resultTestRecord.getAvgValue());
		value.set(sb.toString());
		cxt.write(NullWritable.get(), value);
	}
	
}
