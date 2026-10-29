package in.codifi.clickhouse.logs.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

@JsonIgnoreProperties(ignoreUnknown = true)
@Getter
@Setter
public class EmailLogModel {

    private String userId;
    private String email;
    private String otp;
    private String emailHost;
    private String emailPort;
    private String emailFrom;
    private String emailSubject;
    private String emailContent;
    private String emailResponse;
    private String emailStatus;
    private String createdBy;
    private String source;
    private String filler;
}
