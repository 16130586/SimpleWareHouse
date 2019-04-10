package com.company;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

import com.company.db.DBConnectionUtil;
import com.company.db.MySqlDBConnection;
import com.company.file.FileStatus;

public class LoadingFromStagingToWareHouse {
	static Calendar vn = Calendar.getInstance(TimeZone.getTimeZone("Asia/Bangkok"));

	public static void main(String[] args) throws SQLException {
		DBConnectionUtil cnUtil = new MySqlDBConnection(MySqlDBConnection.STAGING_URL, MySqlDBConnection.USER_NAME,
				MySqlDBConnection.PASSWORD);
		Connection cnn = cnUtil.get();
		String queryStagingReadyForLoadingToWareHouse = "SELECT  distinct st.host_id , st.id, config.staging_table , config.warehouse_require_cols, config.warehouse_cols,config.warehouse_table"
				+ " FROM CONTROL.log_status st inner join CONTROL.host_config config on  st.host_id=config.id"
				+ " WHERE st.file_status='extracting_suc'";
		ResultSet ready_staging_information = cnn.createStatement()
				.executeQuery(queryStagingReadyForLoadingToWareHouse);
		int log_id = -1, host_id = -1;

		String from_staging_table = "", warehouse_require_cols = "", warehouse_cols = "", to_warehouse_table = "";
		while (ready_staging_information.next()) {
			log_id = ready_staging_information.getInt("id");
			host_id = ready_staging_information.getInt("host_id");
			from_staging_table = ready_staging_information.getString("staging_table");
			warehouse_require_cols = ready_staging_information.getString("warehouse_require_cols");
			warehouse_cols = ready_staging_information.getString("warehouse_cols");
			to_warehouse_table = ready_staging_information.getString("warehouse_table");

			String queryGetDataFromCurrentStaging = "".concat("SELECT ").concat("id," + warehouse_require_cols)
					.concat(" FROM STAGING.").concat(from_staging_table);
			ResultSet dataRequiredForWareHouse = cnn.createStatement().executeQuery(queryGetDataFromCurrentStaging);
			int insertedToWareHouseRecords = 0;
			while (dataRequiredForWareHouse.next()) {
				int natural_key = dataRequiredForWareHouse.getInt("natural_key");
				String queryIfExist = "SELECT * FROM WAREHOUSE." + to_warehouse_table + " WHERE natural_key="
						+ natural_key + " AND host_id=" + host_id + " AND is_active=1";
				ResultSet rsOfIfExist = cnn.createStatement().executeQuery(queryIfExist);
				if (rsOfIfExist.next()) {
					// update existed record to inactive
					String queryUpdateOldRecordToInActive = "UPDATE WAREHOUSE." + to_warehouse_table
							+ " SET is_active=0,changed_time='"
							+ (new Timestamp(System.currentTimeMillis()).toString()).substring(0, 19) + "'"
							+ " WHERE natural_key=" + natural_key + " AND host_id=" + host_id + " AND is_active=1";
					cnn.createStatement().executeUpdate(queryUpdateOldRecordToInActive);
					insertedToWareHouseRecords--;
				}
				// insert new record
				String insertNewRecord = "INSERT INTO WAREHOUSE." + to_warehouse_table + "(" + warehouse_cols + ")"
						+ " SELECT " + warehouse_require_cols + " FROM STAGING." + from_staging_table + " WHERE id="
						+ dataRequiredForWareHouse.getInt("id");
				try {
					cnn.createStatement().executeUpdate(insertNewRecord);
					insertedToWareHouseRecords++;
				} catch (Exception e) {
					System.out.println(e.getMessage());
				}
			}
			cnn.createStatement()
					.executeUpdate("UPDATE CONTROL.log_status SET warehouse_record=" + insertedToWareHouseRecords
							+ ", file_status='" + FileStatus.LOADED_SUC.name().toLowerCase() + "'" + " WHERE id="
							+ log_id);
			cnn.createStatement().execute("TRUNCATE TABLE " + from_staging_table);
		}

	}
}
