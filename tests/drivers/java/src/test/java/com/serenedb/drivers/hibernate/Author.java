package com.serenedb.drivers.hibernate;

import jakarta.persistence.CascadeType;
import jakarta.persistence.CollectionTable;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Embedded;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.OrderBy;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Version;
import org.hibernate.annotations.DynamicUpdate;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Entity
@DynamicUpdate
public class Author {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "author_seq")
    @SequenceGenerator(name = "author_seq", sequenceName = "author_seq", allocationSize = 10)
    public Long id;

    @Column(nullable = false, unique = true, length = 100)
    public String name;

    public LocalDate born;

    @Embedded
    public Address address;

    @Version
    public int version;

    @OneToMany(mappedBy = "author", cascade = CascadeType.ALL, orphanRemoval = true)
    @OrderBy("title")
    public List<Book> books = new ArrayList<>();

    @ElementCollection
    @CollectionTable(name = "author_alias")
    @Column(name = "alias", length = 50)
    public Set<String> aliases = new HashSet<>();

    public Author() {}

    public Author(String name) {
        this.name = name;
    }

    public Book write(String title) {
        Book book = new Book();
        book.title = title;
        book.author = this;
        books.add(book);
        return book;
    }
}
