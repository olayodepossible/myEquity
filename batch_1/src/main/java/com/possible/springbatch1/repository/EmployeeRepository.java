package com.possible.springbatch1.repository;

import com.possible.springbatch1.model.Employee;
import org.springframework.data.repository.CrudRepository;

public interface EmployeeRepository extends CrudRepository<Employee, Integer> {
}
