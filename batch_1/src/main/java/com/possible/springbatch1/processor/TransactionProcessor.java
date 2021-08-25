package com.possible.springbatch1.processor;

import com.possible.springbatch1.dto.TransactionDTO;
import com.possible.springbatch1.model.Transaction;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class TransactionProcessor implements ItemProcessor<TransactionDTO, Transaction> {

    @Override
    public Transaction process(TransactionDTO transactionDTO) throws Exception {
        return Transaction.builder()
                .fileSegment(transactionDTO.getFileSegment())
                .fileId(transactionDTO.getFileId())
                .payExt(transactionDTO.getPayExt())
                .pexr(transactionDTO.getPexr())
                .payment(transactionDTO.getPayment())
                .bankName(transactionDTO.getBankName())
                .country(transactionDTO.getCountry())
                .transactionOrder(transactionDTO.getOrder())
                .sap1(transactionDTO.getSap1())
                .build();
    }
}
