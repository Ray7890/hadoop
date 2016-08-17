package test;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * @author Raymond
 * @date 2016-8-03
 */
public class MyKnnCombiner extends Reducer<IntWritable, MyTestRecord, IntWritable, MyTestRecord> {
	
	@Override// 更新k个紧邻值
	public void reduce(IntWritable key, Iterable<MyTestRecord> values,Context cxt) 
			throws IOException, InterruptedException{
		MyTestRecord resultTestRecord=null;
		// 合并相同测试记录的最临近训练记录集
		for(MyTestRecord t:values){
			if(resultTestRecord ==null)
				resultTestRecord = t;
			else{
				resultTestRecord.merge(t);
			}
		}
		
		cxt.write(key, resultTestRecord);
	}

}
