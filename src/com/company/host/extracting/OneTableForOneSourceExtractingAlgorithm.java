package com.company.host.extracting;

import com.company.db.DBConnectionUtil;
import com.company.db.MySqlDBConnection;
import com.company.host.HostConfiguration;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class OneTableForOneSourceExtractingAlgorithm implements ExtractAlgorithm {
	@Override
	// 2.6.1 Extracting algorithm
	public int extract(HostConfiguration host, String fileName, int logId) throws SQLException {
		// 	// 2.6.1.1 Connecting to staging database	
		DBConnectionUtil cnUtil = new MySqlDBConnection(MySqlDBConnection.STAGING_URL, MySqlDBConnection.USER_NAME,
				MySqlDBConnection.PASSWORD);
		// first create table if not exist
		// 2.6.1.3  Creating createIfNotExistDB query
		String createIfNotExistDB = buildCreateQuery(host);

		// create dynamic ~ just kidding
		Connection cnn = cnUtil.get();
		Statement stmt = cnn.createStatement();
		// 2.6.1.4 Executing createIfNotExistDB query
		int createdTable = stmt.executeUpdate(createIfNotExistDB);
		if (createdTable > 0) {
			// creating index
			cnn.createStatement().executeUpdate("CREATE INDEX " + host.getStagingTable() + "_auto_inc_id"
					+ " ON STAGING." + host.getStagingTable() + "(id)");
		}
		// 2.6.1.5 Creating load_file_query from data_config to targeted staging table 
		String fullPathFile = host.getLocalDir() + File.separator + fileName;
		String loadInFileQuery = "LOAD DATA LOCAL INFILE '" + fullPathFile + "'" + " INTO TABLE "
				+ host.getStagingTable() + " FIELDS TERMINATED BY '" + host.getDelim() + "'"
				+ " LINES TERMINATED BY '\n'" + " IGNORE 1 ROWS SET id=NULL, log_id=" + logId + ",host_id="
				+ host.getHostId();
		// then load just like simple :)))

		int insertedRecord = -1;
		// 2.6.1.6 Executing load_file_query
		insertedRecord = stmt.executeUpdate(loadInFileQuery);
		// select so simple :)
		cnUtil.close(cnn);
		// 2.6.1.8 Returning inserted records from executing query process
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

}
