package com.possible.springbatch1.job;

import com.possible.springbatch1.dto.EmployeeDTO;
import com.possible.springbatch1.mapper.EmployeeFileRowMapper;
import com.possible.springbatch1.model.Employee;
import com.possible.springbatch1.processor.EmployeeProcessor;
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
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import javax.sql.DataSource;

@Configuration
public class MultiThreadJob {
    private JobBuilderFactory jobBuilderFactory;
    private StepBuilderFactory stepBuilderFactory;
    private EmployeeProcessor employeeProcessor;
    private DataSource dataSource;
    private CustomEmployeeDBWriter customEmployeeDBWriter;


    public MultiThreadJob(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, EmployeeProcessor employeeProcessor, CustomEmployeeDBWriter customEmployeeDBWriter, DataSource dataSource) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.employeeProcessor = employeeProcessor;
        this.customEmployeeDBWriter = customEmployeeDBWriter;
        this.dataSource = dataSource;
    }

    @Bean("batch3")
    public Job batchOneJob(){
        return this.jobBuilderFactory.get("batch3")
                .start(batchThreadStep1())
                .build();
    }

    @Bean
    public Step batchThreadStep1() {
        return this.stepBuilderFactory.get("multi-thread-step")
                .<EmployeeDTO, Employee>chunk(5)
                .reader(employeeThreadReader())
                .processor(employeeProcessor)
                .writer(customEmployeeDBWriter)
                .taskExecutor(taskExecutor())
                .build();
    }



    @Bean
    @StepScope
    Resource inputFileResourceThread(@Value("#{jobParameters[fileName]}") final String fileName){
        return new ClassPathResource(fileName);
    }

    @Bean
    @StepScope
    public FlatFileItemReader<EmployeeDTO> employeeThreadReader(){
        FlatFileItemReader<EmployeeDTO> reader = new FlatFileItemReader<EmployeeDTO>();
        reader.setResource(inputFileResourceThread(null));
        reader.setLineMapper(new DefaultLineMapper<EmployeeDTO>(){{
            setLineTokenizer(new DelimitedLineTokenizer(){{
                setNames("firstName", "lastName", "email", "age");
            }});
            setFieldSetMapper(new EmployeeFileRowMapper());
        }});
        return reader;
    }


    @Bean
    public TaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor simpleAsyncTaskExecutor = new SimpleAsyncTaskExecutor();
        simpleAsyncTaskExecutor.setConcurrencyLimit(5);
        return simpleAsyncTaskExecutor;
    }
}
