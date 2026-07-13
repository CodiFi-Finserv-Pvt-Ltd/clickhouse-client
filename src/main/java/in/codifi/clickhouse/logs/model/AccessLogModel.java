package in.codifi.clickhouse.logs.model;

import java.sql.Timestamp;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

@JsonIgnoreProperties(ignoreUnknown = true)
@Getter
@Setter
public class AccessLogModel {

    private String userId;
    private String ucc;
    private String uri;
    private String module;
    private String method;
    private Integer statusCode;
    private String source;
    private String reqBody;
    private String resBody;
    private String deviceIp;
    private String deviceId;
    private String reqId;
    private String userAgent;
    private String domain;
    private String contentType;
    private String session;
    private Timestamp inTime;
    private Timestamp outTime;
    private String vendor;
    private String filler1;
    private String filler2;
    private String podId;
}
