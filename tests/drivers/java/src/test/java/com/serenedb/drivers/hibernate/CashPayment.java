package com.serenedb.drivers.hibernate;

import jakarta.persistence.Entity;

@Entity
public class CashPayment extends Payment {

    public String currency;
}
