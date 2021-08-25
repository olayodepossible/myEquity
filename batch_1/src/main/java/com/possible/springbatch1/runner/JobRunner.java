package com.possible.springbatch1.runner;

import com.possible.springbatch1.utils.Constants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;

import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
@Slf4j
public class JobRunner {

    private JobLauncher simpleJobLauncher;
    private Job batchJob;


    public JobRunner(JobLauncher simpleJobLauncher, @Qualifier("batch-flat") Job batchJob) {
        this.simpleJobLauncher = simpleJobLauncher;
        this.batchJob = batchJob;
    }

    @Async
    public String runBatchJob1(){
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addDate("date", new Date(), true);
        return runJob(batchJob, jobParametersBuilder.toJobParameters());
    }

    @Async
    public String runBatchJob2(){
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(Constants.FILE_NAME_CONTEXT_KEY, "data/IDOC_0034216_BULKUP_S02126.txt");
        jobParametersBuilder.addDate("date", new Date(), true);
        return runJob(batchJob, jobParametersBuilder.toJobParameters());
    }

    public String runJob(Job batchJob1, JobParameters jobParameters) {
        try {
            JobExecution jobExecution = simpleJobLauncher.run(batchJob1, jobParameters);
            return String.format("Job %s successfully", jobExecution.getStatus());
        }
        catch (JobExecutionAlreadyRunningException ex){
            log.info("Job with fileName={} is already running.", jobParameters.getParameters().get(Constants.FILE_NAME_CONTEXT_KEY));
        } catch (JobRestartException ex){
            log.info("Job with fileName={} was not restarted.", jobParameters.getParameters().get(Constants.FILE_NAME_CONTEXT_KEY));
        } catch (JobInstanceAlreadyCompleteException ex){
            log.info("Job with fileName={} is already completed.", jobParameters.getParameters().get(Constants.FILE_NAME_CONTEXT_KEY));
        } catch (JobParametersInvalidException ex){
            log.info("Invalid job parameters.", jobParameters.getParameters().get(Constants.FILE_NAME_CONTEXT_KEY));
        }

        return "Completed";
    }
}
