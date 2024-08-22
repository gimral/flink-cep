package com.gimral;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

public class TransactionEvent implements Serializable {
    private Integer id;
    private Long amount;
    private String type;

    public TransactionEvent(Integer id, Long amount,String type){
        this.amount = amount;
        this.type = type;
        this.id = id;
    }

    public Long getAmount() {
        return amount;
    }

    public void setAmount(Long amount) {
        this.amount = amount;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionEvent that = (TransactionEvent) o;
        return Objects.equals(id, that.id) && Objects.equals(amount, that.amount) && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, amount, type);
    }
}
