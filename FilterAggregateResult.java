package com.vishwajeet.pda.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class FilterAggregateResult implements Writable, DBWritable, WritableComparable<FilterAggregateResult>{
	
	private String Time_Bands;
	private String Total;
	
	public FilterAggregateResult() {
		super();
	}

	@Override
	public int compareTo(FilterAggregateResult o) {
		return this.getTime_Bands().compareToIgnoreCase(o.getTime_Bands());
	}

	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
		// TODO Auto-generated method stub
		this.Time_Bands = resultSet.getString("Time_Bands");
		this.Total = resultSet.getString("Total");
	}

	@Override
	public void write(PreparedStatement stmt) throws SQLException {
		stmt.setString(1, Time_Bands);
		stmt.setString(2, Total);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.Time_Bands = dataInput.readUTF();
		this.Total = dataInput.readUTF();
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeUTF(getTime_Bands());
		dataOutput.writeUTF(getTotal());
	}

	public FilterAggregateResult(String time_Bands, String total) {
		super();
		Time_Bands = time_Bands;
		Total = total;
	}

	public String getTime_Bands() {
		return Time_Bands;
	}

	public String getTotal() {
		return Total;
	}

}
