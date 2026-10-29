package in.codifi.clickhouse.logs.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

@JsonIgnoreProperties(ignoreUnknown = true)
@Getter
@Setter
public class SmsLogModel {

    private String userId;
    private String mobile;
    private String otp;
    private String url;
    private String requestMessage;
    private String responseMessage;
    private String sentStatus;
    private String createdBy;
    private String source;
    private String filler;
}
