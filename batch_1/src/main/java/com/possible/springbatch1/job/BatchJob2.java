package com.possible.springbatch1.job;

import com.possible.springbatch1.dto.EmployeeDTO;
import com.possible.springbatch1.mapper.EmployeeDBRowMapper;
import com.possible.springbatch1.model.Employee;
import com.possible.springbatch1.writer.CustomFileItemWriter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import javax.sql.DataSource;

@Configuration
@Slf4j
public class BatchJob2 {
    private JobBuilderFactory jobBuilderFactory;
    private StepBuilderFactory stepBuilderFactory;
    private DataSource dataSource;
    private CustomFileItemWriter customFileItemWriter;

    // from DB to csv
    private final Resource outPutFromDB = new FileSystemResource("data/db_data.csv");

    public BatchJob2(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, CustomFileItemWriter customFileItemWriter, DataSource dataSource) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.customFileItemWriter = customFileItemWriter;
        this.dataSource = dataSource;
    }

    @Bean("batch2")
    public Job batchOneJob() throws Exception {
        return this.jobBuilderFactory.get("batch2")
                .start(batchTwoStep1())
                .build();
    }

    @Bean
    public Step batchTwoStep1() throws Exception {
        return this.stepBuilderFactory.get("step-two")
                .<Employee, EmployeeDTO>chunk(5)
                .reader(employeeDBReader())
                .writer(customFileItemWriter())
                .build();
    }

    private ItemWriter< EmployeeDTO> customFileItemWriter() {

        FlatFileItemWriter<EmployeeDTO> writer = new FlatFileItemWriter<>();
        writer.setResource(outPutFromDB);
        writer.setLineAggregator(new DelimitedLineAggregator<EmployeeDTO>(){
            {
                setFieldExtractor(new BeanWrapperFieldExtractor<EmployeeDTO>(){
                    {
                        setNames( new String[]{"firstName", "lastName", "email", "age"});
                    }

                });
            }
        });
        writer.setShouldDeleteIfExists(true);

        return writer;
    }


    // Reading from DB
    @Bean
    public ItemStreamReader<Employee> employeeDBReader() throws Exception {
        JdbcCursorItemReader<Employee> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(dataSource);
        reader.setSql("select * from employee");
        reader.setRowMapper(new EmployeeDBRowMapper());
        return reader;
    }

}
