package com.serenedb.drivers.hibernate;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

@Embeddable
public class Address {

    @Column(length = 80)
    public String street;

    @Column(length = 40)
    public String city;

    public Address() {}

    public Address(String street, String city) {
        this.street = street;
        this.city = city;
    }
}
