package com.possible.springbatch1.mapper;

import com.possible.springbatch1.dto.EmployeeDTO;
import com.possible.springbatch1.dto.TransactionDTO;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.stereotype.Component;
import org.springframework.validation.BindException;

@Component
public class TransactionFileRowMapper implements FieldSetMapper<TransactionDTO> {

    @Override
    public TransactionDTO mapFieldSet(FieldSet fieldSet) throws BindException {
        return TransactionDTO.builder()
                .fileSegment(fieldSet.readString("fileSegment"))
                .fileId(fieldSet.readString("fileId"))
                .sap1(fieldSet.readString("sap1"))
                .pexr(fieldSet.readString("pexr"))
                .payExt(fieldSet.readString("payExt"))
                .bankName(fieldSet.readString(  "bankName"))
                .country(fieldSet.readString(  "country"))
                .payment(fieldSet.readString(  "payment"))
                .order(fieldSet.readString(  "order"))
                .build();
    }
}
