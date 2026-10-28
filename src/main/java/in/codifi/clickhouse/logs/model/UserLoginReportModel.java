package in.codifi.clickhouse.logs.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

@JsonIgnoreProperties(ignoreUnknown = true)
@Getter
@Setter
public class UserLoginReportModel {

    private String userId;
    private String source;
    private String vendor;
    private Integer visitors;
}
