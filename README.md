# go-simple-postgresql-migrate

An extremely simple PostgreSQL migration library. 
Migrations will be stored in plain SQL as text files in local folder `postgresql-migrations`.

## How to get started

Check out the repository, download dependencies and install:

> go mod download; go build; go install

Initialize the database connection with

> ./go-simple-postgresql-migrate init

Now you can create new migrations for the database schema with

> ./go-simple-postgresql-migrate create my new transaction

Apply all migrations to your database with 

> ./go-simple-postgresql-migrate up

Roll-back the most recent migration with 

> ./go-simple-postgresql-migrate down
