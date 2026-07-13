package in.codifi.clickhouse.logs.model;

import java.sql.Timestamp;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

@JsonIgnoreProperties(ignoreUnknown = true)
@Getter
@Setter
public class RestAccessLogModel {

    private String userId;
    private String ucc;
    private String module;
    private String deviceId;
    private String method;
    private Integer statusCode;
    private String url;
    private String reqId;
    private String reqBody;
    private String resBody;
    private Timestamp inTime;
    private Timestamp outTime;
    private Long totalTime;
    private String vendor;
    private String source;
    private String filler1;
    private String filler2;
    private String podId;
}
