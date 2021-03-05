package com.batch.demo.configuration;

import com.batch.demo.entity.Customer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.classify.Classifier;

public class CustomerClassifier implements Classifier<Customer, ItemWriter<? super Customer>> {

    private ItemWriter<Customer> evenItemWriter;
    private ItemWriter<Customer> oddItemWriter;


    public CustomerClassifier(ItemWriter<Customer> evenItemWriter,
                              ItemWriter<Customer> oddItemWriter) {
    this.evenItemWriter=evenItemWriter;
        this.oddItemWriter=oddItemWriter;
    }

    @Override
    public ItemWriter<? super Customer> classify(Customer customer) {
        return customer.getId()<=500?evenItemWriter:oddItemWriter;
    }
}
