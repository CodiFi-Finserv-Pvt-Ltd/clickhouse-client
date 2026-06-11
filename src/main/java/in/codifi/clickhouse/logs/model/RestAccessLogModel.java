package in.codifi.clickhouse.logs.model;

import java.sql.Timestamp;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class RestAccessLogModel {

    private long id;
    private String userId;
    private String url;
    private Timestamp inTime;
    private Timestamp outTime;
    private String totalTime;
    private String module;
    private String method;
    private String reqBody;
    private String resBody;
    private Timestamp createdOn;
    private Timestamp updatedOn;

    private String accountId;
    private String source;
    private String vendor;
    private int statusCode;
    private String tableName;
}