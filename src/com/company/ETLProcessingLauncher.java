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
		// 1.0 Tải xuống file từ máy chủ ftp từ xa, sau đó lưu lại ở local 
		try {
			// 1.1 Khởi tạo đối tượng LogingFileStatus
			log = new MysqlLoggingFileStatus();
			// 1.3 Tải lên danh sách data_config thông qua HostConfigRetriever
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
	// 1.0 Tải xuống file từ máy chủ ftp từ xa, sau đó lưu lại ở local 
	public static void pullingFileFromFtpServers(List<HostConfiguration> ftpHosts, LoggingFileStatus log)
			throws Exception {
		try {
			
			for (HostConfiguration ftpConfigurator : ftpHosts) {
				// starting connect to FTP host
				// 1.4 Kết nối đến FTP Server
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
				// 1.5 Truy cập thư mục, nhận thông tin file csv
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
				// 	1.6 Tạo java stream để tải xuống file csv 
				ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
				for (String fileName : needToPollingFiles) {
					// 1.7 Ghi lại log trước khi tải
					log.beforePollingFile(ftpConfigurator, fileName);
					// 1.8 Bắt đầu quá trình tải nhờ api dựng sẵn của FTPClient
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
							// 1.10 Ghi lại log tải thành công
							log.afterPollingFile(ftpConfigurator, fileName);
						} else {
							// 1.9 Ghi lại log do quá trình tải thất bại
							log.onPollingFileError(ftpConfigurator, fileName);
							System.out.println(ftpConfigurator.getHostName() + " File not found!  " + fileName);
						}
					} catch (IOException e) {
						System.out.println("On connecting error while polling file: " + fileName);
						// 1.9 Ghi lại log do quá trình tải thất bại
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
		// 2.1 Khởi tạo đối tượng extractAlogirthm
		ExtractAlgorithm extractScript = new OneTableForOneSourceExtractingAlgorithm();
		// loading file from local to staging area
		// 2.2 Lặp qua danh sách data_config
		for (HostConfiguration host : ftpHosts) {
			// 2.3 Lấy lên danh sách file sẵn sàng cho quá trình extract tương úng với data_config
			host.setOwnerLogs(log.getLogs(host));
			// starting extracting
			// 2.4 Lặp qua danh sách file
			for (Log hostLog : host.getOwnerLogs()) {
				String fileName = hostLog.getFileName();
				if (extractScript == null)
					continue;
				// 2.5 Ghi log trước khi extract
				log.beforeExtractingProcess(host, fileName);
				// 2.6 Bắt đầu quá trình extract
				try {
					System.out.println();
					System.out.println(host.getHostName() + "  " + host.getUser() + ", start extracting: " + fileName);
					// 2.6.1 Khởi chạy quá trình extract 
					int stagingRecords = extractScript.extract(host, fileName, hostLog.getLogId());
					// 2.8 Ghi log extract thành công
					log.afterExtractingProcessSuccessfully(host, fileName, stagingRecords);
					System.out.println(
							host.getHostName() + "  " + host.getUser() + ", extracting successfully: " + fileName);
				} catch (SQLException ex) {
					System.out.println(host.getHostName() + "  " + host.getUser() + ", error!: " + fileName
							+ "\n On error : " + ex.getMessage());
					System.out.println();
					// 2.7 Ghi lại log do extract không thành công
					log.onExtractingErrorOccus(host, fileName);
				}
			}
		}
	}

	static Calendar vn = Calendar.getInstance(TimeZone.getTimeZone("Asia/Bangkok"));
	// 3.0 Loading data from staging to warehouse
	public static void loadingFromStagingToWareHouse() throws SQLException {
		// 3.1 Kết nối đến Warehouse Database
		DBConnectionUtil cnUtil = new MySqlDBConnection(MySqlDBConnection.WAREHOUSE_URL, MySqlDBConnection.USER_NAME,
				MySqlDBConnection.PASSWORD);
		Connection cnn = cnUtil.get();
		String queryStagingReadyForLoadingToWareHouse = "SELECT  distinct st.host_id , st.id, config.staging_table , config.warehouse_require_cols, config.warehouse_cols,config.warehouse_table"
				+ " FROM CONTROL.log_status st inner join CONTROL.host_config config on  st.host_id=config.id"
				+ " WHERE st.file_status='extracting_suc'";
		// 3.3 Lấy các row về  host có data trong staging để load vào warehouse từ log_status table
		ResultSet ready_staging_information = cnn.createStatement()
				.executeQuery(queryStagingReadyForLoadingToWareHouse);
		int log_id = -1, host_id = -1;

		String from_staging_table = "", warehouse_require_cols = "", warehouse_cols = "", to_warehouse_table = "";
		while (ready_staging_information.next()) {
			// 3.5 Lấy lên thông tin log_id, host_id, staging_table_name,warehouse_require_cols
			//,warehouse_cols,to_warehouse_table_name
			log_id = ready_staging_information.getInt("id");
			host_id = ready_staging_information.getInt("host_id");
			from_staging_table = ready_staging_information.getString("staging_table");
			warehouse_require_cols = ready_staging_information.getString("warehouse_require_cols");
			warehouse_cols = ready_staging_information.getString("warehouse_cols");
			to_warehouse_table = ready_staging_information.getString("warehouse_table");
			System.out.println(host_id + "  " + from_staging_table + "  " + to_warehouse_table);
			// 3.6 Tạo query lấy dữ liệu cần thiết từ table staging tương tới table warehouse tương ứng
			String queryGetDataFromCurrentStaging = "".concat("SELECT ").concat("id," + warehouse_require_cols)
					.concat(" FROM STAGING.").concat(from_staging_table).concat(" AS BASE");
			ResultSet dataRequiredForWareHouse = cnn.createStatement().executeQuery(queryGetDataFromCurrentStaging);
			int insertedToWareHouseRecords = 0;
			int updatedToWareHouseRecords = 0;
			// 3.7 Lấy lên từng dòng dữ liệu từ staging tương ứng
			while (dataRequiredForWareHouse.next()) {
				int natural_key = dataRequiredForWareHouse.getInt("natural_key");
					String queryIfExist = "SELECT * FROM WAREHOUSE." + to_warehouse_table + " WHERE natural_key="
							+ natural_key + " AND host_id=" + host_id + " AND is_active=1";
					ResultSet rsOfIfExist = cnn.createStatement().executeQuery(queryIfExist);
				// 3.9 Kiểm tra trong warehouse tương ứng đã tồn tại dòng dữ liệu này chưa rsOfIfExist.next()
				if (rsOfIfExist.next()) {
					// update existed record to inactive
					String queryUpdateOldRecordToInActive = "UPDATE WAREHOUSE." + to_warehouse_table
							+ " SET is_active=0,changed_time='"
							+ (new Timestamp(System.currentTimeMillis()).toString()).substring(0, 19) + "'"
							+ " WHERE natural_key=" + natural_key + " AND host_id=" + host_id + " AND is_active=1";
					// 3.10 Update cho dòng đó trong wahouse isActive = false
					cnn.createStatement().executeUpdate(queryUpdateOldRecordToInActive);
					// 3.11 Tăng  updatedToWareHouseRecords lên 1
					updatedToWareHouseRecords++;
				}
				// insert new record
				String insertNewRecord = "INSERT INTO WAREHOUSE." + to_warehouse_table + "(" + warehouse_cols + ")"
						+ " SELECT " + warehouse_require_cols + " FROM STAGING." + from_staging_table +  " AS BASE" +  " WHERE id="
						+ dataRequiredForWareHouse.getInt("id");
//				System.out.println(insertedToWareHouseRecords);
				try {
					// 3.12 Chèn dòng đó vào warehouse table tương ứng
					cnn.createStatement().executeUpdate(insertNewRecord);
					// 3.13 Tăng insertedToWareHouseRecords lên 1
					insertedToWareHouseRecords++;
				} catch (Exception e) {
					System.out.println(e.getMessage());
				}
			}
			
			// 3.8.1 Update inserted records and updated records counter from log_status table of CONTROL DATABASE
			cnn.createStatement()
					.executeUpdate("UPDATE CONTROL.log_status SET warehouse_inserted_record=" + insertedToWareHouseRecords 
							+ ", warehouse_updated_record=" + updatedToWareHouseRecords 
							+ ", file_status='" + FileStatus.LOADED_SUC.name().toLowerCase() + "'" + " WHERE id="
							+ log_id);
			// 3.8.2 Truncating table, make sure no existed data for the next time loading
			cnn.createStatement().execute("TRUNCATE TABLE STAGING." + from_staging_table);
		}
		cnUtil.close(cnn);
	}
}
