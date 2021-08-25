package com.possible.springbatch1.writer;

import com.possible.springbatch1.model.Employee;
import com.possible.springbatch1.repository.EmployeeRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
public class CustomEmployeeDBWriter implements ItemWriter<Employee> {

    private EmployeeRepository employeeRepository;

    public CustomEmployeeDBWriter(EmployeeRepository employeeRepository) {
        this.employeeRepository = employeeRepository;
    }

    @Override
    public void write(List<? extends Employee> employees) throws Exception {
        employeeRepository.saveAll(employees);
        log.info("{} - employees saved into database", employees.size());

    }
}
