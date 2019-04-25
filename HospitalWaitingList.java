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

public class HospitalWaitingList implements Writable, DBWritable, WritableComparable<HospitalWaitingList> {

	private String archivalDate;
	private String hospitalGroup;
	private String hospitalHIPE;
	private String hospitalName;
	private String specialityHIPE;
	private String specialityName;
	private String adultChild;
	private String ageProfile;
	private String timeBands;
	private String total;
	private Long id;
	private String caseType;

	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
		this.archivalDate = resultSet.getString("Archive_Date");
		this.hospitalGroup = resultSet.getString("Hospital_Group");
		this.hospitalHIPE = resultSet.getString("Hospital_HIPE");
		this.hospitalName = resultSet.getString("Hospital_Name");
		this.specialityHIPE = resultSet.getString("Speciality_HIPE");
		this.specialityName = resultSet.getString("Speciality_Name");
		this.adultChild = resultSet.getString("Adult_Child");
		this.ageProfile = resultSet.getString("Age_Profile");
		this.timeBands = resultSet.getString("Time_Bands");
		this.total = resultSet.getString("Total");
		this.id = resultSet.getLong("id");
		this.caseType = resultSet.getString("Case_Type");
	}

	@Override
	public void write(PreparedStatement stmnt) throws SQLException {
		stmnt.setString(1, archivalDate);
		stmnt.setString(2, hospitalGroup);
		stmnt.setString(3, hospitalHIPE);
		stmnt.setString(4, hospitalName);
		stmnt.setString(5, specialityHIPE);
		stmnt.setString(6, specialityName);
		stmnt.setString(7, caseType);
		stmnt.setString(8, adultChild);
		stmnt.setString(9, ageProfile);
		stmnt.setString(10, timeBands);
		stmnt.setString(11, total);
		stmnt.setLong(12, id);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		
		this.archivalDate = dataInput.readUTF();
		this.hospitalGroup = dataInput.readUTF();
		this.hospitalHIPE = dataInput.readUTF();
		this.hospitalName = dataInput.readUTF();
		this.specialityHIPE = dataInput.readUTF();
		this.specialityName = dataInput.readUTF();
		this.caseType = dataInput.readUTF();
		this.adultChild = dataInput.readUTF();
		this.ageProfile = dataInput.readUTF();
		this.timeBands = dataInput.readUTF();
		this.total = dataInput.readUTF();
		this.id = dataInput.readLong();

	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		
		dataOutput.writeUTF(getArchivalDate());
		dataOutput.writeUTF(getHospitalGroup());
		dataOutput.writeUTF(hospitalHIPE);
		dataOutput.writeUTF(getHospitalName());
		dataOutput.writeUTF(specialityHIPE);
		dataOutput.writeUTF(getSpecialityName());
		dataOutput.writeUTF(caseType);
		dataOutput.writeUTF(getAdultChild());
		dataOutput.writeUTF(getAgeProfile());
		dataOutput.writeUTF(getTimeBands());
		dataOutput.writeUTF(total);
		dataOutput.writeLong(id);
	}

	public String getArchivalDate() {
		return archivalDate;
	}

	public String getHospitalGroup() {
		return hospitalGroup;
	}

	public String getHospitalHIPE() {
		return hospitalHIPE;
	}

	public String getHospitalName() {
		return hospitalName;
	}

	public String getSpecialityHIPE() {
		return specialityHIPE;
	}

	public String getSpecialityName() {
		return specialityName;
	}

	public String getAdultChild() {
		return adultChild;
	}

	public String getAgeProfile() {
		return ageProfile;
	}

	public String getTimeBands() {
		return timeBands;
	}

	public String getTotal() {
		return total;
	}
	
	public String getCaseType() {
		return total;
	}
	
	public Long getId() {
		return id;
	}

	@Override
	public String toString() {
		return "HospitalWaitingList [archivalDate=" + archivalDate + ", hospitalGroup=" + hospitalGroup
				+ ", hospitalHIPE=" + hospitalHIPE + ", hospitalName=" + hospitalName + ", specialityHIPE="
				+ specialityHIPE + ", specialityName=" + specialityName + ", adultChild=" + adultChild + ", ageProfile="
				+ ageProfile + ", timeBands=" + timeBands + ", total=" + total + "]";
	}

	@Override
	public int compareTo(HospitalWaitingList o) {
		return id.compareTo(o.id);
	}


	public HospitalWaitingList() {
		super();
	}

	public HospitalWaitingList(String archivalDate, String hospitalGroup, String hospitalHIPE, String hospitalName,
			String specialityHIPE, String specialityName, String adultChild, String ageProfile, String timeBands,
			String total, Long id, String caseType) {
		super();
		this.archivalDate = archivalDate;
		this.hospitalGroup = hospitalGroup;
		this.hospitalHIPE = hospitalHIPE;
		this.hospitalName = hospitalName;
		this.specialityHIPE = specialityHIPE;
		this.specialityName = specialityName;
		this.adultChild = adultChild;
		this.ageProfile = ageProfile;
		this.timeBands = timeBands;
		this.total = total;
		this.id = id;
		this.caseType = caseType;
	}

}
