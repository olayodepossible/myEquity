package com.possible.springbatch.batch;

import com.possible.springbatch.entity.User1;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class Processor implements ItemProcessor<User1, User1> {

    @Override
    public User1 process(User1 user) throws Exception {
        System.out.println("***** Enter Processor to process data *****\n");
        return user;
    }
}
