package com.serenedb.drivers.hibernate;

import jakarta.persistence.Entity;

@Entity
public class CardPayment extends Payment {

    public String card;
}
