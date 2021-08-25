package com.possible.springbatch1.controller;

import com.possible.springbatch1.runner.JobRunner;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/run")
public class JobController {

    private JobRunner runner;

    public JobController(JobRunner runner) {
        this.runner = runner;
    }

    @GetMapping("/write")
    public String runWriteJob(){
       return runner.runBatchJob1();

    }

    @GetMapping("/read")
    public String runReadJob(){
       return runner.runBatchJob2();

    }

}
