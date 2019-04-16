package com.company;

import com.company.db.DBConnectionUtil;
import com.company.db.MySqlDBConnection;
import com.company.file.FileStatus;
import com.company.file.Log;
import com.company.file.LoggingFileStatus;
import com.company.file.MysqlLoggingFileStatus;
import com.company.host.HostConfiguration;
import com.company.host.extracting.ExtractAlgorithm;
import com.company.host.extracting.OneTableForOneSourceExtractingAlgorithm;
import com.company.loading.MysqlFTPConfiguratorRetriever;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;

import java.io.*;
import java.net.SocketException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

public class ETLProcessingLauncher {

	public static void main(String[] args) {
		LoggingFileStatus log = null;
		List<HostConfiguration> ftpHosts = null;
		// 1.0 downloading files on remote servers to local storage
		try {
			// 1.1 initialize LogingFileStatus
			log = new MysqlLoggingFileStatus();
			// 1.3 loading data_configs through HostConfiguration
			ftpHosts = new MysqlFTPConfiguratorRetriever().retrieveAll();
			pullingFileFromFtpServers(ftpHosts, log);
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
		// 2.0 Extracting from files to staging
		extractingFileToStaging(ftpHosts, log);

		try {
			// 3.0 Loading data from staging to warehouse
			loadingFromStagingToWareHouse();
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		}

		log.close();
//
	}
	// 1.0 downloading files on remote servers to local storage
	public static void pullingFileFromFtpServers(List<HostConfiguration> ftpHosts, LoggingFileStatus log)
			throws Exception {
		try {
			
			for (HostConfiguration ftpConfigurator : ftpHosts) {
				// starting connect to FTP host
				// 1.4 Connecting to FTP server
				FTPClient ftpClient = new FTPClient();
				System.out.println(ftpConfigurator.getHostName() + "   " + ftpConfigurator.getPort() + "  "
						+ ftpConfigurator.getUser());
				try {
					ftpClient.connect(ftpConfigurator.getHostName(), ftpConfigurator.getPort());
					boolean isLogin = ftpClient.login(ftpConfigurator.getUser(), ftpConfigurator.getPassword());
					if (!isLogin) {
						System.out.println(ftpConfigurator.getHostName() + "  Connect is denied by some how!");
						continue;
					}
				} catch (SocketException e) {
					System.out.println(ftpConfigurator.getHostName() + " is out of connection time!");
					continue;
				} catch (Exception e) {
					System.out.println(ftpConfigurator.getHostName() + " " + e.getMessage());
					continue;
				}
				ftpClient.setConnectTimeout(10000);

				// ready for listing and changing working directory
				ftpClient.enterLocalPassiveMode();
				// 1.5 Accessing working directory and retrieving file 
				String workingPath = ftpConfigurator.getRemoteDir();
				ftpClient.changeWorkingDirectory(workingPath);

				// reached working directory

				// looking up in the database logging to retrieve present file and
				// downloaded_fail
				List<String> needToPollingFiles = log.getFilesToPolling(ftpConfigurator);

				if (needToPollingFiles.isEmpty()) {
					System.out.println(ftpConfigurator.getHostName() + " has nothing to pull!");
					continue;
				}
				
				// end 1.5

				// entering downloading mod
				// 	1.6 Creating java stream to download files on remote server
				ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
				for (String fileName : needToPollingFiles) {
					// 1.7 Write log after downloading file
					log.beforePollingFile(ftpConfigurator, fileName);
					// 1.8 Starting downloading process through build-in FTP Client
					File localFile = new File(ftpConfigurator.getLocalDir() + File.separator + fileName);
					if (localFile.exists())
						localFile.delete();
					else
						localFile.createNewFile();
					OutputStream fileSavingStream = new BufferedOutputStream(new FileOutputStream(localFile));
					try {
						boolean isRetrieved = ftpClient.retrieveFile(fileName, fileSavingStream);
						if (isRetrieved) {
							System.out.println(ftpConfigurator.getHostName() + " downloaded: " + fileName);
							// 1.10 Writing successfully log
							log.afterPollingFile(ftpConfigurator, fileName);
						} else {
							// 1.9 Writing failure log
							log.onPollingFileError(ftpConfigurator, fileName);
							System.out.println(ftpConfigurator.getHostName() + " File not found!  " + fileName);
						}
					} catch (IOException e) {
						System.out.println("On connecting error while polling file: " + fileName);
						// 1.9 Writing failure log
						log.onPollingFileError(ftpConfigurator, fileName);
					}
					fileSavingStream.close();
				}

			}
		} catch (Exception e) {
			throw e;
		}
	}
	// 2.0 Extracting from files to staging
	public static void extractingFileToStaging(List<HostConfiguration> ftpHosts, LoggingFileStatus log) {
		// 2.1 Choosing Extracting Algorithm
		ExtractAlgorithm extractScript = new OneTableForOneSourceExtractingAlgorithm();
		// loading file from local to staging area
		// 2.2 Iterates data_configs
		for (HostConfiguration host : ftpHosts) {
			// 2.3 Getting list of file with download status
			host.setOwnerLogs(log.getLogs(host));
			// starting extracting
			// 2.4 Iterates ready files
			for (Log hostLog : host.getOwnerLogs()) {
				String fileName = hostLog.getFileName();
				if (extractScript == null)
					continue;
				// 2.5 Write log before extracting
				log.beforeExtractingProcess(host, fileName);
				// 2.6 Starting extracting process
				try {
					System.out.println();
					System.out.println(host.getHostName() + "  " + host.getUser() + ", start extracting: " + fileName);
					// 2.6.1 Extracting algorithm
					int stagingRecords = extractScript.extract(host, fileName, hostLog.getLogId());
					// 2.8 Write successfully extracting log
					log.afterExtractingProcessSuccessfully(host, fileName, stagingRecords);
					System.out.println(
							host.getHostName() + "  " + host.getUser() + ", extracting successfully: " + fileName);
				} catch (SQLException ex) {
					System.out.println(host.getHostName() + "  " + host.getUser() + ", error!: " + fileName
							+ "\n On error : " + ex.getMessage());
					System.out.println();
					// 2.7 Write failure extracting log
					log.onExtractingErrorOccus(host, fileName);
				}
			}
		}
	}

	static Calendar vn = Calendar.getInstance(TimeZone.getTimeZone("Asia/Bangkok"));
	// 3.0 Loading data from staging to warehouse
	public static void loadingFromStagingToWareHouse() throws SQLException {
		// 3.1 Connecting to database
		DBConnectionUtil cnUtil = new MySqlDBConnection(MySqlDBConnection.STAGING_URL, MySqlDBConnection.USER_NAME,
				MySqlDBConnection.PASSWORD);
		Connection cnn = cnUtil.get();
		String queryStagingReadyForLoadingToWareHouse = "SELECT  distinct st.host_id , st.id, config.staging_table , config.warehouse_require_cols, config.warehouse_cols,config.warehouse_table"
				+ " FROM CONTROL.log_status st inner join CONTROL.host_config config on  st.host_id=config.id"
				+ " WHERE st.file_status='extracting_suc'";
		// 3.3 Getting list required informations from staging to warehouse
		ResultSet ready_staging_information = cnn.createStatement()
				.executeQuery(queryStagingReadyForLoadingToWareHouse);
		int log_id = -1, host_id = -1;

		String from_staging_table = "", warehouse_require_cols = "", warehouse_cols = "", to_warehouse_table = "";
		while (ready_staging_information.next()) {
			// 3.5 Getting some variables
			log_id = ready_staging_information.getInt("id");
			host_id = ready_staging_information.getInt("host_id");
			from_staging_table = ready_staging_information.getString("staging_table");
			warehouse_require_cols = ready_staging_information.getString("warehouse_require_cols");
			warehouse_cols = ready_staging_information.getString("warehouse_cols");
			to_warehouse_table = ready_staging_information.getString("warehouse_table");
			
			// 3.6 Creating retrieving required data query
			String queryGetDataFromCurrentStaging = "".concat("SELECT ").concat("id," + warehouse_require_cols)
					.concat(" FROM STAGING.").concat(from_staging_table);
			ResultSet dataRequiredForWareHouse = cnn.createStatement().executeQuery(queryGetDataFromCurrentStaging);
			int insertedToWareHouseRecords = 0;
			
			// 3.7 Getting one row by a time from targeted staging
			while (dataRequiredForWareHouse.next()) {
				int natural_key = dataRequiredForWareHouse.getInt("natural_key");
				String queryIfExist = "SELECT * FROM WAREHOUSE." + to_warehouse_table + " WHERE natural_key="
						+ natural_key + " AND host_id=" + host_id + " AND is_active=1";
				ResultSet rsOfIfExist = cnn.createStatement().executeQuery(queryIfExist);
				// 3.9 Checking existing data in targeted warehouse table
				if (rsOfIfExist.next()) {
					// update existed record to inactive
					String queryUpdateOldRecordToInActive = "UPDATE WAREHOUSE." + to_warehouse_table
							+ " SET is_active=0,changed_time='"
							+ (new Timestamp(System.currentTimeMillis()).toString()).substring(0, 19) + "'"
							+ " WHERE natural_key=" + natural_key + " AND host_id=" + host_id + " AND is_active=1";
					// 3.10 Update isActive to false
					cnn.createStatement().executeUpdate(queryUpdateOldRecordToInActive);
					// 3.11 Increasing updated by 1 - the next time i take this code will be updated :))) LOL
					// just old version of mine
					insertedToWareHouseRecords--;
				}
				// insert new record
				String insertNewRecord = "INSERT INTO WAREHOUSE." + to_warehouse_table + "(" + warehouse_cols + ")"
						+ " SELECT " + warehouse_require_cols + " FROM STAGING." + from_staging_table + " WHERE id="
						+ dataRequiredForWareHouse.getInt("id");
				try {
					// 3.12 Inserting new record to targeted warehouse table
					cnn.createStatement().executeUpdate(insertNewRecord);
					// 3.13 Increasing inserted by 1 -  it's working fine!
					insertedToWareHouseRecords++;
				} catch (Exception e) {
					System.out.println(e.getMessage());
				}
			}
			
			// 3.8.1 Update inserted records and updated records counter from log_status table of CONTROL DATABASE
			cnn.createStatement()
					.executeUpdate("UPDATE CONTROL.log_status SET warehouse_record=" + insertedToWareHouseRecords
							+ ", file_status='" + FileStatus.LOADED_SUC.name().toLowerCase() + "'" + " WHERE id="
							+ log_id);
			// 3.8.2 Truncating table, make sure no existed data for the next time loading
			cnn.createStatement().execute("TRUNCATE TABLE " + from_staging_table);
		}
		cnUtil.close(cnn);
	}
}
