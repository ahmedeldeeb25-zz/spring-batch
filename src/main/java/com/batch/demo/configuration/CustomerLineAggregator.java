package com.batch.demo.configuration;

import com.batch.demo.entity.Customer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.item.file.transform.LineAggregator;

public class CustomerLineAggregator  implements LineAggregator<Customer> {

    private ObjectMapper objectMapper=new ObjectMapper();
    @Override
    public String aggregate(Customer customer) {
        try {
            System.out.println(customer);
            return objectMapper.writeValueAsString(customer);
        }catch(JsonProcessingException e){
            throw new RuntimeException("Unable to serialize Customer",e);
        }
    }
}
