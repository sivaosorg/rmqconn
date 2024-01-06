# rmqconn

![GitHub contributors](https://img.shields.io/github/contributors/sivaosorg/gocell)
![GitHub followers](https://img.shields.io/github/followers/sivaosorg)
![GitHub User's stars](https://img.shields.io/github/stars/pnguyen215)

A simple RabbitMQ service implementation with support for declaring/removing exchanges, queues, producing messages, and consuming messages.

## Table of Contents

- [rmqconn](#rmqconn)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Modules](#modules)
    - [Running Tests](#running-tests)
    - [Tidying up Modules](#tidying-up-modules)
    - [Upgrading Dependencies](#upgrading-dependencies)
    - [Cleaning Dependency Cache](#cleaning-dependency-cache)

## Introduction

This repository contains a RabbitMQ service implementation with essential functionalities for handling exchanges, queues, producing, and consuming messages. It is designed to be a lightweight and flexible solution for integrating RabbitMQ into your projects.

## Prerequisites

Golang version v1.20

## Installation

- Latest version

```bash
go get -u github.com/sivaosorg/rmqconn@latest
```

- Use a specific version (tag)

```bash
go get github.com/sivaosorg/rmqconn@v0.0.1
```

## Modules

Explain how users can interact with the various modules.

### Running Tests

To run tests for all modules, use the following command:

```bash
make test
```

### Tidying up Modules

To tidy up the project's Go modules, use the following command:

```bash
make tidy
```

### Upgrading Dependencies

To upgrade project dependencies, use the following command:

```bash
make deps-upgrade
```

### Cleaning Dependency Cache

To clean the Go module cache, use the following command:

```bash
make deps-clean-cache
```
