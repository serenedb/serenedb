package com.serenedb.drivers.hibernate;

import jakarta.persistence.Entity;

@Entity
public class Video extends Media {

    public int minutes;
}
