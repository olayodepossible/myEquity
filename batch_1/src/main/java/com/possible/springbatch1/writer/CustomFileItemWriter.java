package com.possible.springbatch1.writer;

import com.possible.springbatch1.dto.EmployeeDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
public class CustomFileItemWriter implements ItemWriter<EmployeeDTO> {
    // from DB to csv
    private final Resource outPutFromDB = new FileSystemResource("data/db_data.csv");

    @Override
    public void write(List<? extends EmployeeDTO> list) throws Exception {
        System.out.println("Calling Writer");
        FlatFileItemWriter<EmployeeDTO> writer = new FlatFileItemWriter<>();
        writer.setResource(outPutFromDB);
        writer.setLineAggregator(new DelimitedLineAggregator<EmployeeDTO>(){
            {
                setFieldExtractor(new BeanWrapperFieldExtractor<EmployeeDTO>(){
                    {
                        setNames( new String[]{"firstName", "lastName", "email", "age"});
                    }

                });
            }
        });
        writer.setShouldDeleteIfExists(true);

        log.info("write data into {}", outPutFromDB);

    }
}
