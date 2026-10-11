package com.serenedb.drivers.hibernate;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.SequenceGenerator;

@Entity
public class Hero {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "hero_seq")
    @SequenceGenerator(name = "hero_seq", sequenceName = "hero_seq", allocationSize = 10)
    public Long id;

    @ManyToOne(optional = false)
    public Guild guild;
}
