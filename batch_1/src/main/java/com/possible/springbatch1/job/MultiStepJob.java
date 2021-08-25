package com.possible.springbatch1.job;

import com.possible.springbatch1.dto.EmployeeDTO;
import com.possible.springbatch1.mapper.EmployeeDBRowMapper;
import com.possible.springbatch1.mapper.EmployeeFileRowMapper;
import com.possible.springbatch1.model.Employee;
import com.possible.springbatch1.processor.EmployeeProcessor;
import com.possible.springbatch1.writer.CustomEmployeeDBWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import javax.sql.DataSource;

@Configuration
public class MultiStepJob {
    private JobBuilderFactory jobBuilderFactory;
    private StepBuilderFactory stepBuilderFactory;
    private EmployeeProcessor employeeProcessor;
    private DataSource dataSource;
    private CustomEmployeeDBWriter customEmployeeDBWriter;

    // from DB to csv
    private final Resource outPutFromDB = new FileSystemResource("data/db_data.csv");


    public MultiStepJob(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, EmployeeProcessor employeeProcessor, CustomEmployeeDBWriter customEmployeeDBWriter, DataSource dataSource) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.employeeProcessor = employeeProcessor;
        this.customEmployeeDBWriter = customEmployeeDBWriter;
        this.dataSource = dataSource;
    }

    @Bean("batch4")
    public Job batchOneJob(){
        return this.jobBuilderFactory.get("batch4")
                .start(batchOneStep())
                .next(batchTwoStep2())
                .build();
    }

    @Bean
    public Step batchOneStep() {
        return this.stepBuilderFactory.get("step1")
                .<EmployeeDTO, Employee>chunk(5)
                .reader(employeeCSVReader())
                .processor(employeeProcessor)
                .writer(customEmployeeDBWriter)
                .build();
    }

    @Bean
    public Step batchTwoStep2() {
        return this.stepBuilderFactory.get("step-two")
                .<Employee, EmployeeDTO>chunk(5)
                .reader(employeeDBDataReader())
                .writer(customCSVFileItemWriter())
                .build();
    }

    @Bean
    @StepScope
    Resource inputFileResources(@Value("#{jobParameters[fileName]}") final String fileName){
        return new ClassPathResource(fileName);
    }

    @Bean
    @StepScope
    public FlatFileItemReader<EmployeeDTO> employeeCSVReader(){
        FlatFileItemReader<EmployeeDTO> reader = new FlatFileItemReader<EmployeeDTO>();
        reader.setResource(inputFileResources(null));
        reader.setLineMapper(new DefaultLineMapper<EmployeeDTO>(){{
            setLineTokenizer(new DelimitedLineTokenizer(){{
                setNames("firstName", "lastName", "email", "age");
            }});
            setFieldSetMapper(new EmployeeFileRowMapper());
        }});
        return reader;
    }


    private ItemWriter< EmployeeDTO> customCSVFileItemWriter() {

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
    public ItemStreamReader<Employee> employeeDBDataReader() {
        JdbcCursorItemReader<Employee> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(dataSource);
        reader.setSql("select * from employee");
        reader.setRowMapper(new EmployeeDBRowMapper());
        return reader;
    }
}
