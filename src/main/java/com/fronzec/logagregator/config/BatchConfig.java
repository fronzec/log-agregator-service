package com.fronzec.logagregator.config;

import com.fronzec.logagregator.model.LogData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.NonNull;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import javax.sql.DataSource;

@Configuration
public class BatchConfig {

    private static final Logger logger = LoggerFactory.getLogger(BatchConfig.class);

    @Bean
    public JobLauncher asyncJobLauncher(JobRepository jobRepository) throws Exception {
        TaskExecutorJobLauncher jobLauncher = new TaskExecutorJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
        jobLauncher.afterPropertiesSet();
        return jobLauncher;
    }

    @Bean
    public JobLauncher syncJobLauncher(JobRepository jobRepository) throws Exception {
        TaskExecutorJobLauncher jobLauncher = new TaskExecutorJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.afterPropertiesSet();
        return jobLauncher;
    }

    private static class StringToLocalDateTimeConverter implements Converter<String, LocalDateTime> {
        private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

        @Override
        @NonNull
        public LocalDateTime convert(@NonNull String source) {
            return LocalDateTime.parse(source, formatter);
        }
    }

    private static class LoggingWriteListener implements ChunkListener {
        @Override
        public void beforeChunk(@NonNull ChunkContext context) {
            logger.info("About to process chunk");
        }

        @Override
        public void afterChunk(@NonNull ChunkContext context) {
            logger.info("Successfully processed chunk");
            long count = context.getStepContext().getStepExecution().getReadCount();
            long writeCount = context.getStepContext().getStepExecution().getWriteCount();
            logger.info("Read {} items in this chunk", count);
            logger.info("Wrote {} items in this chunk", writeCount);
        }

        @Override
        public void afterChunkError(@NonNull ChunkContext context) {
            logger.error("Error processing chunk. Items read: {}", 
                context.getStepContext().getStepExecution().getReadCount());
        }
    }

    @Bean
    @StepScope
    public FlatFileItemReader<LogData> reader(@Value("#{jobParameters['input-file-path']}") String filePath) {
        FlatFileItemReader<LogData> reader = new FlatFileItemReader<>();
        reader.setResource(new ClassPathResource(filePath));
        reader.setLinesToSkip(1);

        DefaultLineMapper<LogData> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("timestamp", "level", "service", "message");

        BeanWrapperFieldSetMapper<LogData> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(LogData.class);

        // Configure custom conversion service
        DefaultConversionService conversionService = new DefaultConversionService();
        conversionService.addConverter(new StringToLocalDateTimeConverter());
        fieldSetMapper.setConversionService(conversionService);

        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        reader.setLineMapper(lineMapper);
        return reader;
    }

    @Bean
    @StepScope
    public JdbcBatchItemWriter<LogData> writer(DataSource dataSource) {
        JdbcBatchItemWriter<LogData> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(dataSource);
        writer.setSql("INSERT INTO log_data (timestamp, level, service, message) VALUES (:timestamp, :level, :service, :message)");
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        return writer;
    }

    @Bean
    @JobScope
    public Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                     FlatFileItemReader<LogData> reader, JdbcBatchItemWriter<LogData> writer) {
        return new StepBuilder("step1", jobRepository)
                .<LogData, LogData>chunk(10, transactionManager)
                .reader(reader)
                .writer(writer)
                .listener(new LoggingWriteListener())
                .build();
    }

    @Bean
    public Job importLogDataJob(JobRepository jobRepository, Step step1) {
        return new JobBuilder("importLogDataJob", jobRepository)
                .start(step1)
                .build();
    }
}
