package com.possible.springbatch1.job;

import com.possible.springbatch1.dto.TransactionDTO;
import com.possible.springbatch1.mapper.TransactionFileRowMapper;
import com.possible.springbatch1.model.Transaction;
import com.possible.springbatch1.processor.TransactionProcessor;
import com.possible.springbatch1.writer.CustomEmployeeDBWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import javax.sql.DataSource;

@Configuration
public class BatchJobFlat {
    private JobBuilderFactory jobBuilderFactory;
    private StepBuilderFactory stepBuilderFactory;
    private TransactionProcessor transactionProcessor;
    private DataSource dataSource;


    public BatchJobFlat(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, TransactionProcessor transactionProcessor, DataSource dataSource) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.transactionProcessor = transactionProcessor;
        this.dataSource = dataSource;
    }

    @Bean("batch-flat")
    public Job batchOneJob3(){
        return this.jobBuilderFactory.get("batch-flat")
                .start(batchOneStepFlat())
                .build();
    }

    @Bean
    public Step batchOneStepFlat() {
        return this.stepBuilderFactory.get("step-flat")
                .<TransactionDTO, Transaction>chunk(5)
                .reader(transactionReader())
                .processor(transactionProcessor)
                .writer(transactionDBWriter())
                .build();
    }

    @Bean
    @StepScope
    Resource inputFileResourceFlat(@Value("#{jobParameters[fileName]}") final String fileName){
        return new ClassPathResource(fileName);
    }

    @Bean
    @StepScope
    public FlatFileItemReader<TransactionDTO> transactionReader(){
        FlatFileItemReader<TransactionDTO> reader = new FlatFileItemReader<TransactionDTO>();
        reader.setResource(inputFileResourceFlat(null));
        reader.setLineMapper(new DefaultLineMapper<TransactionDTO>(){{
            setLineTokenizer(new FixedLengthTokenizer() {{
                setNames("fileSegment", "fileId", "sap1", "pexr", "payment", "order", "payExt", "bankName", "country");
                setColumns(new Range(1, 8), new Range(14, 22), new Range(33, 37), new Range(43, 67), new Range(67, 78), new Range(79, 101), new Range(102, 113), new Range(113, 144), new Range(148, 155));
                setStrict(false);
            }});
            setFieldSetMapper(new TransactionFileRowMapper());
        }});
        return reader;
    }


    @Bean
    public JdbcBatchItemWriter<Transaction> transactionDBWriter(){
        JdbcBatchItemWriter<Transaction> itemWriter = new JdbcBatchItemWriter<>();
        itemWriter.setDataSource(dataSource);
        itemWriter.setSql("insert into transaction (file_segment, file_id, sap1, pexr, pay_ext, bank_name, country, payment, transaction_order) values( :fileSegment, :fileId, :sap1, :pexr, :payExt, :bankName, :country, :payment, :transactionOrder)");
        itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Transaction>());
        return itemWriter;
    }
}
