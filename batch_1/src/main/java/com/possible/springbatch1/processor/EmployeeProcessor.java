package com.possible.springbatch1.processor;

import com.possible.springbatch1.model.Employee;
import com.possible.springbatch1.dto.EmployeeDTO;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class EmployeeProcessor implements ItemProcessor<EmployeeDTO, Employee> {

    @Override
    public Employee process(EmployeeDTO employeeDTO) throws Exception {
        return Employee.builder()
                .firstName(employeeDTO.getFirstName())
                .lastName(employeeDTO.getLastName())
                .email(employeeDTO.getEmail())
                .age(employeeDTO.getAge())
                .build();
    }
}
