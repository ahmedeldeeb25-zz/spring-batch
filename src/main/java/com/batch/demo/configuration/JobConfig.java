package com.batch.demo.configuration;

import com.batch.demo.configuration.mapper.CustomerRowMapper;
import com.batch.demo.entity.Customer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.support.ClassifierCompositeItemProcessor;
import org.springframework.batch.item.support.ClassifierCompositeItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import javax.sql.DataSource;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@EnableBatchProcessing
public class JobConfig {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    public DataSource dataSource;

    @Bean
    public JdbcPagingItemReader<Customer> pagingItemReader(){
        JdbcPagingItemReader<Customer> reader = new JdbcPagingItemReader<>();

        reader.setDataSource(this.dataSource);
        reader.setFetchSize(10);
        reader.setRowMapper(new CustomerRowMapper());

        MySqlPagingQueryProvider queryProvider=new MySqlPagingQueryProvider();
        queryProvider.setSelectClause("id, first_name, last_name, birthdate, email, bank");
        queryProvider.setFromClause("from customer");

        Map<String, Order> sortKeys=new HashMap(1);

        sortKeys.put("id",Order.ASCENDING);
        queryProvider.setSortKeys(sortKeys);
        reader.setQueryProvider(queryProvider);

        return reader;
    }

    @Bean
    public FlatFileItemWriter<Customer> jsonItemWriter() throws Exception{
        FlatFileItemWriter<Customer> itemWriter=new FlatFileItemWriter<>();

        itemWriter.setLineAggregator(new CustomerLineAggregator());
        String customerOutputPath = File.createTempFile("customerOut",".out").getAbsolutePath();
        System.out.println(">> Output Path: "+customerOutputPath);
        itemWriter.setResource(new FileSystemResource(customerOutputPath));
        itemWriter.afterPropertiesSet();

        return itemWriter;
    }

    @Bean
    public FlatFileItemWriter<Customer> jsonItemWriter2() throws Exception{
        FlatFileItemWriter<Customer> itemWriter=new FlatFileItemWriter<>();

        itemWriter.setLineAggregator(new CustomerLineAggregator());
        String customerOutputPath = File.createTempFile("customerOut2",".out").getAbsolutePath();
        System.out.println(">> Output Path: "+customerOutputPath);
        itemWriter.setResource(new FileSystemResource(customerOutputPath));
        itemWriter.afterPropertiesSet();

        return itemWriter;
    }



    @Bean
    public CompositeItemWriter<Customer> itemWriter() throws Exception {
        List<ItemWriter<? super Customer>> writers=new ArrayList<>(2);

        writers.add(jsonItemWriter());
        writers.add(jsonItemWriter2());

        CompositeItemWriter<Customer> itemWriter=new CompositeItemWriter<>();

        itemWriter.setDelegates(writers);
        itemWriter.afterPropertiesSet();

        return  itemWriter;
    }

    @Bean
    public ClassifierCompositeItemWriter<Customer> dynamicItemWriter() throws Exception{
        ClassifierCompositeItemWriter<Customer> itemWriter=new ClassifierCompositeItemWriter<>();

        itemWriter.setClassifier(new CustomerClassifier(jsonItemWriter(),jsonItemWriter2()));

        return itemWriter;
    }

    public UpperCaseProcessor itemProcessor(){
        return new UpperCaseProcessor();
    }

    @Bean
    public  ItemWriter<Customer> customerItemWriter(){
        return items ->{
            for(Customer item:items){
                System.out.println(item.toString());
            }
        };
    }


    @Bean
    public Step step1() throws Exception {
        return stepBuilderFactory.get("step1")
                .<Customer,Customer>chunk(10)
                .reader(pagingItemReader())
                .writer(dynamicItemWriter())
                .processor(itemProcessor())
               // .writer(customerItemWriter())
                .stream(jsonItemWriter())// in cause you're using dynamic itemWriter()
                .stream(jsonItemWriter2())
                .build();
    }

    @Bean
    public Step step2() {
        return stepBuilderFactory.get("step2")
                .tasklet((StepContribution contribution, ChunkContext chunkContext) ->{
                        System.out.println("Hello World 2 !");
                        return RepeatStatus.FINISHED;
                    }
                ).build();
    }
//    @Bean
//    public Job helloWorldJob() {
//        return jobBuilderFactory.get("helloWorldJob")
//                .start(step1())
//                .next(step2())
//                .next(step3())
//                .build();
//    }

    @Bean
    public Job JobTransition() throws Exception {
        return jobBuilderFactory.get("job101") // make sure it's unique each time you run
                .start(step1())
                .build();
    }
}
