package com.company.file;

import com.company.host.HostConfiguration;

import java.util.List;

public interface LoggingFileStatus {

    void beforeExtractingProcess(HostConfiguration config, String fileName);

    void afterExtractingProcessSuccessfully(HostConfiguration config, String fileName, int staggingRecord);

    void onExtractingErrorOccus(HostConfiguration config, String fileName);

    List<String> getFilesToPolling(HostConfiguration ftpConfigurator);

    void beforePollingFile(HostConfiguration config, String fileName);

    void afterPollingFile(HostConfiguration config, String fileName);

    void onPollingFileError(HostConfiguration config, String FileName);

    List<Log> getLogs(HostConfiguration config);

    void close();
}
