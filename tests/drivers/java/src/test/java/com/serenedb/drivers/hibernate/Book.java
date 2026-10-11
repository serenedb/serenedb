package com.serenedb.drivers.hibernate;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.JoinTable;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import org.hibernate.annotations.DynamicUpdate;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

@Entity
@DynamicUpdate
@Table(indexes = @Index(name = "book_published_idx", columnList = "published"),
        uniqueConstraints = @UniqueConstraint(name = "book_author_title_uk", columnNames = {"author_id", "title"}))
public class Book {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "book_seq")
    @SequenceGenerator(name = "book_seq", sequenceName = "book_seq", allocationSize = 10)
    public Long id;

    @ManyToOne(optional = false)
    public Author author;

    @Column(nullable = false, length = 200)
    public String title;

    @Column(precision = 8, scale = 2)
    public BigDecimal price;

    public Integer pages;

    public OffsetDateTime published;

    @Enumerated(EnumType.STRING)
    @Column(length = 20)
    public Genre genre;

    public UUID uid;

    public byte[] cover;

    public Duration readingTime;

    @ManyToMany
    @JoinTable(name = "book_tag")
    public Set<Tag> tags = new HashSet<>();
}
