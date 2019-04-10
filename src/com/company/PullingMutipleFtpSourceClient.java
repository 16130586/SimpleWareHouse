package com.company;

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
import java.sql.SQLException;
import java.util.List;

public class PullingMutipleFtpSourceClient {

	public static void main(String[] args) {
		LoggingFileStatus log = new MysqlLoggingFileStatus();
		List<HostConfiguration> ftpHosts = null;
		try {
			// loading all configurator in the control database
			ftpHosts = new MysqlFTPConfiguratorRetriever().retrieveAll();
			for (HostConfiguration ftpConfigurator : ftpHosts) {
				// starting connect to ftp host
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

				// entering downloading mod
				ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
				for (String fileName : needToPollingFiles) {
					log.beforePollingFile(ftpConfigurator, fileName);
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
							log.afterPollingFile(ftpConfigurator, fileName);
						} else {
							log.onPollingFileError(ftpConfigurator, fileName);
							System.out.println(ftpConfigurator.getHostName() + " File not found!  " + fileName);
						}
					} catch (IOException e) {
						System.out.println("On connecting error while polling file: " + fileName);
						log.onPollingFileError(ftpConfigurator, fileName);
					}
					fileSavingStream.close();
				}

			}
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
		ExtractAlgorithm extractScript = new OneTableForOneSourceExtractingAlgorithm();
		// loading file from local to staging area
		for (HostConfiguration host : ftpHosts) {
			host.setOwnerLogs(log.getLogs(host));
			// starting extracting
			for (Log hostLog : host.getOwnerLogs()) {
				String fileName = hostLog.getFileName();
				if (extractScript == null)
					continue;
				log.beforeExtractingProcess(host, fileName);
				try {
					System.out.println();
					System.out.println(host.getHostName() + "  " + host.getUser() + ", start extracting: " + fileName);
					int stagingRecords = extractScript.extract(host, fileName , hostLog.getLogId());
					log.afterExtractingProcess(host, fileName, stagingRecords);
					System.out.println(
							host.getHostName() + "  " + host.getUser() + ", extracting successfully: " + fileName);
				} catch (SQLException ex) {
					System.out.println(host.getHostName() + "  " + host.getUser() + ", error!: " + fileName
							+ "\n On error : " + ex.getMessage());
					System.out.println();
					log.onExtractingErrorOccus(host, fileName);
				}
			}
		}
		log.close();
//
	}
}
