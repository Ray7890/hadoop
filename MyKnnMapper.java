package test;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.LineReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Raymond
 * @date 2016-8-12
 * */
public class MyKnnMapper extends Mapper<LongWritable, Text, IntWritable, MyTestRecord> {
	static Logger log = LoggerFactory.getLogger(MyKnnMapper.class);
	private ArrayList<MyTestRecord> testRecords= new ArrayList<MyTestRecord>();
	private int K;
	
	@Override // 读取test数据集
	public void setup(Context cxt) throws IOException{
		Configuration conf= cxt.getConfiguration();
		this.K = conf.getInt("K",3);
		String testPath= conf.get("TestFilePath");
		Path testDataPath= new Path(testPath);
		FileSystem fs = testDataPath.getFileSystem(conf);
		readTestData(fs,testDataPath);
	}
	@Override
	public void map(LongWritable key,Text value,Context cxt) throws NumberFormatException, IOException, InterruptedException{
		String[] line= value.toString().split(",");
		for(MyTestRecord t: testRecords){
			MyTrainRecord trainRecord = new MyTrainRecord(t, line);
			t.addTrainRecord(trainRecord);
		}
	}
	
	@Override
	public void cleanup(Context cxt) throws IOException,InterruptedException{
		IntWritable id = new IntWritable();
		// 遍历输出,构造Key:Value
		for(int i=0;i<testRecords.size();i++){
			id.set(i);
			cxt.write(id, testRecords.get(i));
		}
	}

	/**
	 * @param fs
	 * @param Path
	 * @throws IOException 
	 */
	// 读取测试数据集
	private void readTestData(FileSystem fs,Path path) throws IOException {
		LineReader in = new LineReader(fs.open(path));
		Text line = new Text();
		while(in.readLine(line)>0){
			String[] testData= line.toString().split(",");
			float[] attribute= new float[testData.length];
			for(int i=0;i<testData.length;i++){
				attribute[i]=Float.parseFloat(testData[i]);
			}
			MyTestRecord testRecord = new MyTestRecord(attribute, this.K);
			testRecords.add(testRecord);
		}
		in.close();
	}

}
