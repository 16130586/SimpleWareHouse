package com.company.file;

import com.company.db.MySqlDBConnection;
import com.company.host.HostConfiguration;

import java.sql.*;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

public class MysqlLoggingFileStatus implements LoggingFileStatus {
	private Connection cnn;

	public MysqlLoggingFileStatus() {
		super();
		cnn = new MySqlDBConnection().get();

	}

	public void afterExtractingProcessSuccessfully(HostConfiguration config, String fileName, int stagingRecords) {
		String insertNewLogQuery = "UPDATE log_status SET file_status=?, stagging_record=?,end_extracting_date=? WHERE host_id=? AND file_name=?";
		try {
			PreparedStatement stmt = cnn.prepareStatement(insertNewLogQuery);
			stmt.setString(1, FileStatus.EXTRACTING_SUC.name().toLowerCase());
			stmt.setInt(2, stagingRecords);
			Calendar vn = Calendar.getInstance(TimeZone.getTimeZone("Asia/Bangkok"));
			stmt.setTimestamp(3, new Timestamp(System.currentTimeMillis()), vn);
			stmt.setInt(4, config.getHostId());
			stmt.setString(5, fileName);
			stmt.executeUpdate();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	public void onExtractingErrorOccus(HostConfiguration config, String fileName) {
		String insertNewLogQuery = "UPDATE log_status SET file_status=? , end_extracting_date=? WHERE host_id=? AND file_name=?";
		try {
			PreparedStatement stmt = cnn.prepareStatement(insertNewLogQuery);
			stmt.setString(1, FileStatus.EXTRACTING_FAIL.name().toLowerCase());
			Calendar vn = Calendar.getInstance(TimeZone.getTimeZone("Asia/Bangkok"));
			stmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()), vn);
			stmt.setInt(3, config.getHostId());
			stmt.setString(4, fileName);
			stmt.executeUpdate();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	public void beforeExtractingProcess(HostConfiguration config, String fileName) {
		String insertNewLogQuery = "UPDATE log_status SET file_status=? , start_extracting_date=? WHERE host_id=? AND file_name=?";
		try {
			PreparedStatement stmt = cnn.prepareStatement(insertNewLogQuery);
			stmt.setString(1, FileStatus.EXTRACTING.name().toLowerCase());
			Calendar vn = Calendar.getInstance(TimeZone.getTimeZone("Asia/Bangkok"));
			stmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()), vn);
			stmt.setInt(3, config.getHostId());
			stmt.setString(4, fileName);
			stmt.executeUpdate();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
	}

	@Override
	public List<String> getFilesToPolling(HostConfiguration ftpConfigurator) {
		String query = "SELECT t1.file_name FROM log_status t1 " + "left join log_status t2 "
				+ "on t1.host_id=t2.host_id and t1.file_name=t2.file_name "
				+ "and t1.file_status = t2.file_status and t1.file_status=t2.file_status and t1.uploaded_date < t2.uploaded_date "
				+ "WHERE t2.host_id is null and t1.host_id=? and (t1.file_status=? or t1.file_status=?)";
		List<String> fileNames = new LinkedList<>();
		try {
			PreparedStatement stmt = cnn.prepareStatement(query);
			stmt.setInt(1, ftpConfigurator.getHostId());
			stmt.setString(2, FileStatus.PRESENT.name().toLowerCase());
			stmt.setString(3, FileStatus.DOWNLOADED_FAIL.name().toLowerCase());
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				fileNames.add(rs.getString("file_name"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return fileNames;
	}

	@Override
	public void beforePollingFile(HostConfiguration config, String fileName) {
		String query = "UPDATE log_status SET file_status=?, start_polling_date=? " + "WHERE host_id=? AND file_name=?";
		try {
			PreparedStatement stmt = cnn.prepareStatement(query);
			stmt.setString(1, FileStatus.DOWNLOADING.name().toLowerCase());
			Calendar vn = Calendar.getInstance(TimeZone.getTimeZone("Asia/Bangkok"));
			stmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()), vn);
			stmt.setInt(3, config.getHostId());
			stmt.setString(4, fileName);
			stmt.executeUpdate();
		} catch (SQLException ex) {
			ex.printStackTrace();
		}
	}

	@Override
	public void afterPollingFile(HostConfiguration config, String fileName) {
		String query = "UPDATE log_status SET file_status=?, end_polling_date=? " + "WHERE host_id=? AND file_name=?";
		try {
			PreparedStatement stmt = cnn.prepareStatement(query);
			stmt.setString(1, FileStatus.DOWNLOADED.name().toLowerCase());
			Calendar vn = Calendar.getInstance(TimeZone.getTimeZone("Asia/Bangkok"));
			stmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()), vn);
			stmt.setInt(3, config.getHostId());
			stmt.setString(4, fileName);
			stmt.executeUpdate();
		} catch (SQLException ex) {
			ex.printStackTrace();
		}
	}

	@Override
	public void onPollingFileError(HostConfiguration config, String fileName) {
		String query = "UPDATE log_status SET file_status=?, end_polling_date=? " + "WHERE host_id=? AND file_name=?";
		try {
			PreparedStatement stmt = cnn.prepareStatement(query);
			stmt.setString(1, FileStatus.DOWNLOADED_FAIL.name().toLowerCase());
			Calendar vn = Calendar.getInstance(TimeZone.getTimeZone("Asia/Bangkok"));
			stmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()), vn);
			stmt.setInt(3, config.getHostId());
			stmt.setString(4, fileName);
			stmt.executeUpdate();
		} catch (SQLException ex) {
			ex.printStackTrace();
		}
	}

	@Override
	public List<Log> getLogs(HostConfiguration config) {
		List<Log> logs = new LinkedList<>();
		String query = "SELECT * FROM log_status where host_id=?"
				+ " and (file_status=? or start_extracting_date is null or end_extracting_date is null)";
		try {
			PreparedStatement stmt = cnn.prepareStatement(query);
			stmt.setInt(1, config.getHostId());
			stmt.setString(2, FileStatus.DOWNLOADED.name().toLowerCase());
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				logs.add(new Log(rs.getInt("id"), rs.getInt("host_id"), rs.getString("file_name")));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return logs;
	}

	@Override
	public void close() {
		if (cnn != null) {
			try {
				cnn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
