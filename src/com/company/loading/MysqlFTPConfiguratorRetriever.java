package com.company.loading;

import com.company.db.DBConnectionUtil;
import com.company.db.MySqlDBConnection;
import com.company.host.HostConfiguration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class MysqlFTPConfiguratorRetriever implements HostConfigRetriever {
    public List<HostConfiguration> retrieveAll() throws SQLException {
        DBConnectionUtil util = new MySqlDBConnection(MySqlDBConnection.CONTROL_URL, MySqlDBConnection.USER_NAME, MySqlDBConnection.PASSWORD);
        Connection cnn = util.get();

        List<HostConfiguration> result = new ArrayList<HostConfiguration>();
        String selectAll = "SELECT * FROM host_config where isActive<>0";
        PreparedStatement pre = cnn.prepareStatement(selectAll);
        ResultSet set = pre.executeQuery();
        while (set.next()) {
            HostConfiguration configer = new HostConfiguration();
            configer.setHostId(Integer.parseInt(set.getString("id")));
            configer.setHostName(set.getString("host_name"));
            configer.setPort(set.getInt("port"));
            configer.setUser(set.getString("user_name"));
            configer.setPassword(set.getString("user_password"));
            configer.setRemoteDir(set.getString("remote_dir"));
            configer.setLocalDir(set.getString("local_dir"));
            configer.setFormatFile(set.getString("name_file_format"));
            configer.setTransformScriptName(set.getString("transform_script_name"));
            configer.setExtractScriptName(set.getString("extract_script_name"));
            configer.setHostListColumns(set.getString("host_list_cols"));
            configer.setStagingTable(set.getString("staging_table"));
            configer.setDelim(set.getString("delim"));
            configer.setWarehouseRequiredColumns(set.getString("warehouse_require_cols"));
            result.add(configer);
        }

        util.close(cnn);
        return result;
    }
}
