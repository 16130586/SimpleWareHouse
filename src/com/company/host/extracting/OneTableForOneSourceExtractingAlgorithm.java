package com.company.host.extracting;

import com.company.db.DBConnectionUtil;
import com.company.db.MySqlDBConnection;
import com.company.host.HostConfiguration;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.spi.DateFormatProvider;
import java.time.LocalDate;
import java.util.Date;

import org.apache.commons.net.ntp.TimeStamp;

public class OneTableForOneSourceExtractingAlgorithm implements ExtractAlgorithm {
	@Override
	// 2.6.1 Extracting algorithm
	public int extract(HostConfiguration host, String fileName, int logId) throws SQLException {
		// 	// 2.6.1.1 Kết nối đến staging database	
		DBConnectionUtil cnUtil = new MySqlDBConnection(MySqlDBConnection.STAGING_URL, MySqlDBConnection.USER_NAME,
				MySqlDBConnection.PASSWORD);
		// first create table if not exist
		// 2.6.1.3  Tạo query tạo bảng nếu chưa có từ data_config_columns
		String createIfNotExistDB = buildCreateQuery(host);

		// create dynamic ~ just kidding
		Connection cnn = cnUtil.get();
		Statement stmt = cnn.createStatement();
		// 2.6.1.4 Khởi chạy createIfNotExistDB query
		int createdTable = stmt.executeUpdate(createIfNotExistDB);
		if (createdTable > 0) {
			// creating index
			cnn.createStatement().executeUpdate("CREATE INDEX " + host.getStagingTable() + "_auto_inc_id"
					+ " ON STAGING." + host.getStagingTable() + "(id)");
		}
		// 2.6.1.5 Tạo query load file từ data_config vào table tương ứng
		String fullPathFile = host.getLocalDir() + File.separator + fileName;
		String loadInFileQuery = "LOAD DATA LOCAL INFILE '" + fullPathFile + "'" + " INTO TABLE "
				+ host.getStagingTable() + " FIELDS TERMINATED BY '" + host.getDelim() + "'"
				+ " LINES TERMINATED BY '\n'" + " IGNORE 1 ROWS SET id=NULL, log_id=" + logId + ",host_id="
				+ host.getHostId();
		// then load just like simple :)))

		int insertedRecord = -1;
		// 2.6.1.6 Khởi chạy load_file query
		insertedRecord = stmt.executeUpdate(loadInFileQuery);
		// select so simple :)
		cnUtil.close(cnn);
		// 2.6.1.8 Trả về số record được chèn vào bẳng tương ứng
		return insertedRecord;
	}
	// 2.6.1.3  Creating createIfNotExistDB query
	private String buildCreateQuery(HostConfiguration host) {
		StringBuilder bd = new StringBuilder();
		String[] listColumns = host.getHostListColumns().split(",");
		bd.append("CREATE TABLE IF NOT EXISTS ");
		bd.append(host.getStagingTable());
		bd.append("(");

		for (int i = 0; i < listColumns.length; i++) {
			bd.append(listColumns[i] + " text,");
		}
		bd.append("id int AUTO_INCREMENT primary key");
		bd.append(")");
		bd.append(" CHARACTER SET utf8mb4");
		System.out.println(bd.toString());
		return bd.toString();
	}
	public static void main(String[] args) throws SQLException {
		DBConnectionUtil cnUtil = new MySqlDBConnection(MySqlDBConnection.WAREHOUSE_URL, MySqlDBConnection.USER_NAME,
				MySqlDBConnection.PASSWORD);
		Connection cn = cnUtil.get();
		String insertSql = "INSERT INTO WAREHOUSE.date(DATE_SK, FULL_DATE , DATE_OF_MONTH,MONTH_OF_YEAR,YEAR) VALUES(?,?,?,?,?)";
		PreparedStatement stmt = cn.prepareStatement(insertSql);
		
		LocalDate startDate = LocalDate.of(1990, 1, 1);
		LocalDate endDate = LocalDate.of(2000, 1, 1);
		int sk = 1;
		while(startDate.compareTo(endDate) < 0) {
			stmt.setInt(1, sk++);
			stmt.setTimestamp(2, new Timestamp(startDate.getYear(), startDate.getMonthValue(), startDate.getDayOfMonth(), 0, 0, 0, 0));
			stmt.setInt(3, startDate.getDayOfMonth());
			stmt.setInt(4, startDate.getMonthValue());
			stmt.setInt(5, startDate.getYear());
			stmt.executeUpdate();
			startDate  = startDate.plusDays(1);
		}
		
		
	}

}
