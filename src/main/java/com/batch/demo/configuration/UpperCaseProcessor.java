package com.batch.demo.configuration;

import com.batch.demo.entity.Customer;
import org.springframework.batch.item.ItemProcessor;

public class UpperCaseProcessor implements ItemProcessor<Customer,Customer> {
    @Override
    public Customer process(Customer customer) throws Exception {
        return new Customer(customer.getId(),
                customer.getFirst_name().toUpperCase(),
                customer.getLast_name().toUpperCase(),
                customer.getEmail(),
                customer.getBirthdate(),
                customer.getBank());
    }
}
