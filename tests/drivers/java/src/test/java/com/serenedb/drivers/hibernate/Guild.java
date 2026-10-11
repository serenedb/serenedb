package com.serenedb.drivers.hibernate;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;

@Entity
public class Guild {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "guild_seq")
    @SequenceGenerator(name = "guild_seq", sequenceName = "guild_seq", allocationSize = 10)
    public Long id;

    @Column(nullable = false, unique = true, length = 50)
    public String name;

    public String motto;
}
