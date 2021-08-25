package com.possible.springbatch;

import com.possible.springbatch.entity.Transaction;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.batch.item.xml.builder.StaxEventItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;


@EnableBatchProcessing
@SpringBootApplication
public class SpringBatchApplication {
    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    @StepScope
    public FlatFileItemReader<Transaction> fileTransactionReader(@Value("${inputFlatFile}")Resource resource) {
        return new FlatFileItemReaderBuilder<Transaction>()
                .name("transactionItemReader")
                .resource(resource)
                .delimited()
                .names(new String[]{"account, amount, timestamp"})
                .fieldSetMapper(fieldSet -> {
                    Transaction transaction = new Transaction();
                    transaction.setAccount(fieldSet.readString("account"));
                    transaction.setAmount(fieldSet.readBigDecimal("amount"));
                    transaction.setTimestamp(fieldSet.readDate("timestamp", "yyyy-MM-dd HH:mm:ss"));
                    return transaction;
                })
                .build();

    }

    @Bean
    @StepScope
    public StaxEventItemReader<Transaction> xmlTransactionReader(@Value("#{jobParameters['inputXmlFile']}")Resource resource){
        Jaxb2Marshaller unmarshaller = new Jaxb2Marshaller();
        unmarshaller.setClassesToBeBound(Transaction.class);

        return new StaxEventItemReaderBuilder<Transaction>()
                .name("xmlFileTransactionReader")
                .resource(resource)
                .addFragmentRootElements("transaction")
                .unmarshaller(unmarshaller)
                .build();
    }
    // Async
   /* @Bean("Async")
    public ItemProcessor<Transaction, Transaction> processor(){
        return (transaction -> {
            Thread.sleep(5);
            return transaction;
        });
    }*/

  /*  @Bean
    public AsyncItemProcessor<Transaction, Transaction> asyncItemProcessor(){
        AsyncItemProcessor<Transaction, Transaction> processor = new AsyncItemProcessor<>();
        processor.setDeligate(processor());
        processor.setTaskExecutor(new SimpleAsyncTaskExecutor());
        return processor;
    }

    @Bean
    public AsyncItemWriter<Transaction> asyncItemWriter(){
        AsyncItemWriter<Transaction> writer = new AsyncItemWriter<>();

        writer.setDeligate(null);
        return writer;
    }

    @Bean
    public Step step1Async(){
        return this.stepBuilderFactory.get("step1Async")
                .<Transaction, Transaction>chunk(100)
                .reader(fileTransactionReader(null))
                .processor((ItemProcessor) asyncItemProcessor())
                .writer(asyncItemWriter())
                .build();
    }
*/
    //end Async

    @Bean
    @StepScope
    public JdbcBatchItemWriter<Transaction> writer(DataSource dataSource){
        return new JdbcBatchItemWriterBuilder<Transaction>()
                .dataSource(dataSource)
                .sql("INSERT INTO transaction (ACCOUNT, AMOUNT, TIMESTAMP) VALUES (:account, :amount, :timestamp)")
                .beanMapped()
                .build();
    }


// Parallel batch Job
    /*@Bean
    public Job parallelStepsJob(){
        Flow secondFlow = new FlowBuilder<Flow>("secondFlow")
                .start(step2())
                .build();

        Flow firstFlow = new FlowBuilder<Flow>("firstFlow")
                .start(step1())
                .split(new SimpleAsyncTaskExecutor())
                .add(secondFlow)
                .build();

        return this.jobBuilderFactory.get("parallelStepsJob")
                .start(firstFlow)
                .end()
                .build();
    }*/

// Sequence batch Job
   /* @Bean
    public Job sequentialStepsJob(){
        return this.jobBuilderFactory.get("sequentialStepsJob")
                .start(step1())
                .next(step2())
                .build();
    }*/


// MultiThreading batch job
    @Bean
    public Job multiThreadedJob(){
        return this.jobBuilderFactory.get("multiThreadedJob")
                .incrementer(new RunIdIncrementer())
                .start(step1())
                .build();
    }
    @Bean
    public Step step1(){
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(4);
        taskExecutor.setMaxPoolSize(4);
        taskExecutor.afterPropertiesSet();


        return this.stepBuilderFactory.get("step1")
                .<Transaction, Transaction> chunk(100)
                .reader(fileTransactionReader(null))
                .writer(writer(null))
                .taskExecutor(taskExecutor)
                .build();
    }

    @Bean
    public Step step2(){
        return this.stepBuilderFactory.get("step2")
                .<Transaction, Transaction> chunk(100)
                .reader(xmlTransactionReader(null))
                .writer(writer(null))
                .build();
    }

    public static void main(String[] args) {
        /*String [] newArgs = new String[] {"inputFlatFile=/data/csv/transaction_file.csv",
                                            "inputXmlFile=/data/xml/big_trnx.xml"};*/
        SpringApplication.run(SpringBatchApplication.class, args);
    }

}
