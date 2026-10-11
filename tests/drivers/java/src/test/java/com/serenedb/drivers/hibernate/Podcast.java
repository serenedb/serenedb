package com.serenedb.drivers.hibernate;

import jakarta.persistence.Entity;

@Entity
public class Podcast extends Media {

    public String host;
}
