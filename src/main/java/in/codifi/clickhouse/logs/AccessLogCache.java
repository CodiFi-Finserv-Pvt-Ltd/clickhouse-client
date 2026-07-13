package in.codifi.clickhouse.logs;

import java.util.ArrayList;
import java.util.List;

import in.codifi.clickhouse.logs.model.AccessLogModel;
import in.codifi.clickhouse.logs.model.RestAccessLogModel;

public class AccessLogCache {

    private final List<RestAccessLogModel> batchList          = new ArrayList<>();
    private final List<AccessLogModel>     accessLogBatchList = new ArrayList<>();

    long restQueueBytes   = 0L;
    long accessQueueBytes = 0L;

    public List<RestAccessLogModel> getBatchList() {
        return batchList;
    }

    public List<AccessLogModel> getAccessLogBatchList() {
        return accessLogBatchList;
    }
}
