package net.javaguides.springboot.payload;


import lombok.Data;

@Data
public class Transaction {
    private String transactionIdentifier;
    private String accountIBAN;
    private String amount;
    private String description;
    private String valueDate;
}
