package com.vishwajeet.pda;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.vishwajeet.pda.model.FilterAggregateResult;
import com.vishwajeet.pda.model.HospitalWaitingList;

public class MySQLToMySQLFilter extends Configured implements Tool{

	public static void main(String[] args) {
		int exitcode = 0;
		try {
			exitcode = ToolRunner.run(new MySQLToMySQLFilter(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
        System.exit(exitcode);

	}
	
	public static class Map extends Mapper<LongWritable, HospitalWaitingList, Text , HospitalWaitingList> {
		@Override
		public void map(LongWritable key, HospitalWaitingList record, Context context) throws IOException, InterruptedException {
			if (record.getSpecialityHIPE() != null) {
				if(record.getAdultChild().equalsIgnoreCase("Child") && (record.getAgeProfile().trim().equalsIgnoreCase("16-64") ||  record.getAgeProfile().trim().equalsIgnoreCase("65+"))) {
					// do nothing
				} else if(record.getAdultChild().equalsIgnoreCase("Adult") && (record.getAgeProfile().trim().equalsIgnoreCase("0-15"))) {
					// do nothing
				} else {
					//// group by
					if(record.getTimeBands().strip().equalsIgnoreCase("9-12 Months")
							|| record.getTimeBands().strip().equalsIgnoreCase("12-15 Months")
							|| record.getTimeBands().strip().equalsIgnoreCase("18+ Months")) {
						context.write(new Text(record.getTimeBands().strip()), record);
					}
				}
				
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, HospitalWaitingList, FilterAggregateResult, NullWritable> {
		@Override
		public void reduce(Text key, Iterable<HospitalWaitingList> values, Context context) throws IOException, InterruptedException {
			Double totalOfTotals = 0d;
			for (HospitalWaitingList hospitalWaitingList : values) {
				totalOfTotals = totalOfTotals + Double.parseDouble(hospitalWaitingList.getTotal().strip());
			}
			context.write(new FilterAggregateResult(key.toString(), totalOfTotals.toString()), NullWritable.get());
		}
	}
	
	public int run(String[] arg0) throws Exception {
		
		Configuration conf = new Configuration();
		
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver",
				"jdbc:mysql://localhost:3306/mapreduce?user=root&password=password");
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(MySQLToMySQLFilter.class);
		job.setJobName("MySQLToMySQLFilter");
		
		job.setInputFormatClass(DBInputFormat.class);
		job.setOutputFormatClass(DBOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(HospitalWaitingList.class);
		
		job.setOutputKeyClass(FilterAggregateResult.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		DBInputFormat.setInput(job, HospitalWaitingList.class,
				"select Archive_Date,Hospital_Group,Hospital_HIPE,Hospital_Name,Speciality_HIPE,Speciality_Name,Case_Type,Adult_Child,Age_Profile,Time_Bands,Total,id" + 
				" from WAITING_LIST_2018", "select 138330");

		//DBOutputFormat.setOutput(job, "WAITING_LIST_2018_RESULTS", "Archive_Date,Hospital_Group,Hospital_HIPE,Hospital_Name,Speciality_HIPE,Speciality_Name,Case_Type,Adult_Child,Age_Profile,Time_Bands,Total,id");
		DBOutputFormat.setOutput(job, "WAITING_LISTS_AGG_RESULTS", "Time_Bands,Total");
		
		return job.waitForCompletion(true)? 0 : 1;
	}
	
}
