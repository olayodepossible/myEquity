package com.possible.springbatch1.job;

import com.possible.springbatch1.model.Employee;
import com.possible.springbatch1.dto.EmployeeDTO;
import com.possible.springbatch1.mapper.EmployeeFileRowMapper;
import com.possible.springbatch1.processor.EmployeeProcessor;
import com.possible.springbatch1.writer.CustomEmployeeDBWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import javax.sql.DataSource;

@Configuration
public class BatchJob1 {
    private JobBuilderFactory jobBuilderFactory;
    private StepBuilderFactory stepBuilderFactory;
    private EmployeeProcessor employeeProcessor;
    private DataSource dataSource;
    private CustomEmployeeDBWriter customEmployeeDBWriter;


    public BatchJob1(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, EmployeeProcessor employeeProcessor, CustomEmployeeDBWriter customEmployeeDBWriter, DataSource dataSource) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.employeeProcessor = employeeProcessor;
        this.customEmployeeDBWriter = customEmployeeDBWriter;
        this.dataSource = dataSource;
    }

    @Bean("batch1")
    public Job batchOneJob(){
        return this.jobBuilderFactory.get("batch1")
                .start(batchOneStep1())
                .build();
    }

    @Bean
    public Step batchOneStep1() {
        return this.stepBuilderFactory.get("step1")
                .<EmployeeDTO, Employee>chunk(5)
                .reader(employeeReader())
                .processor(employeeProcessor)
                .writer(customEmployeeDBWriter)
                .build();
    }

    @Bean
    @StepScope
    Resource inputFileResource(@Value("#{jobParameters[fileName]}") final String fileName){
        return new ClassPathResource(fileName);
    }

    @Bean
    @StepScope
    public FlatFileItemReader<EmployeeDTO> employeeReader(){
        FlatFileItemReader<EmployeeDTO> reader = new FlatFileItemReader<EmployeeDTO>();
        reader.setResource(inputFileResource(null));
        reader.setLineMapper(new DefaultLineMapper<EmployeeDTO>(){{
            setLineTokenizer(new DelimitedLineTokenizer(){{
                setNames("firstName", "lastName", "email", "age");
            }});
            setFieldSetMapper(new EmployeeFileRowMapper());
        }});
        return reader;
    }


    @Bean
    public JdbcBatchItemWriter<Employee> employeeDBWriter(){
        JdbcBatchItemWriter<Employee> itemWriter = new JdbcBatchItemWriter<>();
        itemWriter.setDataSource(dataSource);
        itemWriter.setSql("insert into employee (first_name, last_name, email, age) values( :firstName, :lastName, :email, :age)");
        itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Employee>());
        return itemWriter;
    }
}
