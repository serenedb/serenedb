package com.serenedb.drivers.hibernate;

import jakarta.persistence.DiscriminatorValue;
import jakarta.persistence.Entity;

@Entity
@DiscriminatorValue("bike")
public class Bike extends Vehicle {

    public int gears;
}
