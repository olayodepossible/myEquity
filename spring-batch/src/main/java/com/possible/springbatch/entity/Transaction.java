package com.possible.springbatch.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.xml.bind.annotation.XmlRootElement;
import java.math.BigDecimal;
import java.util.Date;

//@Entity
@Data
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "transaction")
public class Transaction {
   /* @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id; */
    private String account;
    private Date timestamp;
    private BigDecimal amount;
}
