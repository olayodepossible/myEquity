package com.possible.springbatch1.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TransactionDTO {
    private String fileId;
    private String fileSegment;
    private String sap1;
    private String pexr;
    private String payExt;
    private String bankName;
    private String country;
    private String payment;
    private String order;


}
