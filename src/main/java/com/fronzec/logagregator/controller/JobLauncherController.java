package com.fronzec.logagregator.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/jobs")
public class JobLauncherController {

    private static final Logger logger = LoggerFactory.getLogger(JobLauncherController.class);

    private final JobLauncher syncJobLauncher;
    private final JobLauncher asyncJobLauncher;
    private final Job importLogDataJob;

    public JobLauncherController(JobLauncher syncJobLauncher, JobLauncher asyncJobLauncher, Job importLogDataJob) {
        this.syncJobLauncher = syncJobLauncher;
        this.asyncJobLauncher = asyncJobLauncher;
        this.importLogDataJob = importLogDataJob;
    }

    @PostMapping("/import-logs")
    public ResponseEntity<Map<String, Object>> launchImportJob(
            @RequestParam("filePath") String filePath,
            @RequestParam(value = "sync", defaultValue = "false") boolean sync) {
        Map<String, Object> response = new HashMap<>();

        try {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addString("input-file-path", filePath)
                    .addDate("date", new Date())
                    .toJobParameters();

            var jobExecution = sync 
                ? syncJobLauncher.run(importLogDataJob, jobParameters)
                : asyncJobLauncher.run(importLogDataJob, jobParameters);

            response.put("jobId", jobExecution.getJobId());
            response.put("status", jobExecution.getStatus());
            response.put("startTime", jobExecution.getStartTime());
            response.put("endTime", jobExecution.getEndTime());

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Error launching job: ", e);
            response.put("error", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }
}
