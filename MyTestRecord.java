package test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Writable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Raymond
 * @date 2016-08-12
 */
public class MyTestRecord implements Writable {
	static Logger log = LoggerFactory.getLogger(MyTestRecord.class);
	// 属性数组
	private float[] attributes;
	
	// 最临近训练记录 按距离排好序的
	private List<MyTrainRecord> nearestTrainRecord = new ArrayList<MyTrainRecord>();

	// KNN 算法K
	private int K;

	public MyTestRecord(){}
	
	public MyTestRecord(float[] attributes, int K) {
		this.attributes = attributes;
		this.K = K;
	}

	/*
	 * 增加最临近训练记录
	 */
	public void addTrainRecord(MyTrainRecord trainRecord) {
		if (this.nearestTrainRecord.size() < K)
			this.nearestTrainRecord.add(trainRecord);
		else {
			if (this.nearestTrainRecord.get(K - 1).compareTo(trainRecord) > 0) {
				this.nearestTrainRecord.remove(K - 1);
				this.nearestTrainRecord.add(trainRecord);
			}
			Collections.sort(this.nearestTrainRecord);
		}
	}
	/*
	 * 相同的一条测试数据 合并其最临近训练记录列表
	 */
	public void merge(MyTestRecord testRecord) {
			for(MyTrainRecord t:testRecord.nearestTrainRecord){
				this.addTrainRecord(t);
			}
		}		
	/**
	 * 得到最临近训练记录中的Y值平均数
	 */
		public float getAvgValue() {
			float sum = 0;
			int count = 0;
			for (int i = 0; i < nearestTrainRecord.size()-1; i++) {
				sum=sum+nearestTrainRecord.get(i).getY();
				count ++;
			}				
		return (sum/count);
	}

		// write the output data at the certain order
		// attributes; nearestTrainRecords; K; 
	public void write(DataOutput out) throws IOException {
		out.writeInt(attributes.length);
		for (int i = 0; i < attributes.length; i++) {
			out.writeFloat(attributes[i]);
		}
		out.writeInt(nearestTrainRecord.size());
		for (int i = 0; i < nearestTrainRecord.size(); i++) {
			out.writeFloat(nearestTrainRecord.get(i).getDistance());
			out.writeFloat(nearestTrainRecord.get(i).getY());
		}
		out.writeInt(K);
	}
	// Read the input data accordingly
	// attributes; nearestTrainRecords; K; 
	public void readFields(DataInput in) throws IOException {
		int attributesLength = in.readInt();
		attributes = new float[attributesLength];
		for (int i = 0; i < attributesLength; i++) {
			attributes[i] = in.readFloat();
		}
		int nearestTrainRecordSize = in.readInt();
		this.nearestTrainRecord = new ArrayList<MyTrainRecord>();
		for (int i = 0; i < nearestTrainRecordSize; i++) {
			this.nearestTrainRecord.add(new MyTrainRecord(in.readFloat(), in.readFloat()));
		}
		this.K = in.readInt();
	}	
	public int getK() {
		return K;
	}

	public void setK(int k) {
		K = k;
	}
	public float[] getAttributes() {
		return attributes;
	}

	public void setAttributes(float[] attributes) {
		this.attributes = attributes;
	}

	public List<MyTrainRecord> getNearestTrainRecord() {
		return nearestTrainRecord;
	}

	public void setNearestTrainRecord(List<MyTrainRecord> nearestTrainRecord) {
		this.nearestTrainRecord = nearestTrainRecord;
	}
}
