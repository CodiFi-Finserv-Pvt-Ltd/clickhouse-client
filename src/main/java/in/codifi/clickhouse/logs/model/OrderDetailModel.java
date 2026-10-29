package in.codifi.clickhouse.logs.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

/** All fields are strings; numeric columns are parsed tolerantly on insert (bad value → NULL). */
@JsonIgnoreProperties(ignoreUnknown = true)
@Getter
@Setter
public class OrderDetailModel {

    private String userId;
    private String accId;
    private String user;
    private String token;
    private String exchange;
    private String tradingSymbol;
    private String action;
    private String transType;
    private String product;
    private String validity;
    private String prcType;
    private String orderType;
    private String customerFirm;
    private String qty;
    private String price;
    private String triggerPrice;
    private String discQty;
    private String cancelledSize;
    private String mktProtectionPrice;
    private String orderNo;
    private String target;
    private String stopLossValue;
    private String trailingStopLoss;
    private String tickSize;
    private String ltpOrAtp;
    private String sqrOffAbsOrTick;
    private String slabsOrTick;
    private String trailTickYesNo;
    private String classification;
    private String vendorCode;
    private String flagToAddCategory;
    private String orderCriteria;
    private String posSqrOffFlag;
    private String algoId;
    private String orderTag;
    private String comments;
    private String source;
    private String method;
    private String module;
    private String deviceIp;
    private String deviceId;
    private String status;
    private String rejReason;
    private String createdBy;
    private String activeStatus;
    private String appCode;
    private String sourcePlatform;
    private String filler1;
    private String filler2;
    private String filler3;
    private String filler4;
}
