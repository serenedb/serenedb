---
title: C++
sidebar_position: 4
---

# C++

SereneDB works with [libpqxx](https://pqxx.org/), the official C++ client for PostgreSQL.

## Install

On Ubuntu/Debian:

```sh
sudo apt install libpqxx-dev
```

## Connect

```cpp
#include <pqxx/pqxx>

int main() {
    pqxx::connection conn("host=localhost port=7890");
```

## Create a table and insert data

```cpp
    pqxx::work txn(conn);
    txn.exec(R"(
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY,
            title TEXT,
            views INTEGER
        )
    )");
    txn.exec_params(
        "INSERT INTO articles VALUES ($1, $2, $3)",
        1, "Introduction to Vector Search", 4200
    );
    txn.commit();
```

## Query

```cpp
    pqxx::work txn(conn);
    auto result = txn.exec("SELECT title, views FROM articles ORDER BY views DESC");
    for (auto const& row : result) {
        std::cout << row["title"].as<std::string>()
                  << " — " << row["views"].as<int>() << " views\n";
    }
```

## Build

```sh
g++ -std=c++17 main.cpp -lpqxx -lpq -o client
./client
```
