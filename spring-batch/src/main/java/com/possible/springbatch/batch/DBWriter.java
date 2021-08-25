package com.possible.springbatch.batch;

import com.possible.springbatch.entity.User1;
import com.possible.springbatch.repository.UserRepository;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class DBWriter implements ItemWriter<User1> {

    private final UserRepository userRepository;

    public DBWriter(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @Override
    public void write(List<? extends User1> users) throws Exception {
        System.out.println("***** Data Saved for User *****\n"+ users);
        userRepository.saveAll(users);
    }
}
