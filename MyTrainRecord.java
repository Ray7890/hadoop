package test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Raymond
 * @date 2016-7-15
 * 与测试记录关系的训练数据记录
 * */
public class MyTrainRecord implements Comparable<MyTrainRecord> {
	static Logger log = LoggerFactory.getLogger(MyTrainRecord.class);
	private float distance; // 与对应测试数据集的距离
	private float xValue; // X值
	private float yValue; // Y值
	
	public MyTrainRecord(float distance, float yValue) {
		this.distance = distance;
		this.yValue = yValue;
	}

	public MyTrainRecord(MyTestRecord testRecord, String[] line) {
		// 初始化，属性、距离
	//	float[] attributes = new float[line.length - 1];
		this.xValue = Float.valueOf(line[0]);
		this.yValue = Float.valueOf(line[1]);
		this.distance = java.lang.Math.abs((testRecord.getAttributes()[0] - this.xValue));
	}

	public float getDistance() {
		return distance;
	}
	public float getX() {
		return xValue;
	}
	public float getY() {
		return yValue;
	}
	
	public int compareTo(MyTrainRecord trainRecord) {
		if (this.distance > trainRecord.distance)
			return 1;
		else if (this.distance < trainRecord.distance)
			return -1;
		else
			return 0;
	}
}
