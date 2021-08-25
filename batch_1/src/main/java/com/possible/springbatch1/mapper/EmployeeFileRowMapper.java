package com.possible.springbatch1.mapper;

import com.possible.springbatch1.dto.EmployeeDTO;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.stereotype.Component;
import org.springframework.validation.BindException;

@Component
public class EmployeeFileRowMapper implements FieldSetMapper<EmployeeDTO> {

    @Override
    public EmployeeDTO mapFieldSet(FieldSet fieldSet) throws BindException {
        return EmployeeDTO.builder()
                .firstName(fieldSet.readString("firstName"))
                .lastName(fieldSet.readString("lastName"))
                .email(fieldSet.readString("email"))
                .age(fieldSet.readInt("age"))
                .build();
    }
}
