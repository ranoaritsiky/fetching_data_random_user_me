FROM postgres:latest
ENV POSTGRES_PASSWORD=secret
ENV POSTGRES_USER=postgres
ENV POSTGRES_DB=random_me
COPY create_fixtures.sql /docker-entrypoint-initdb.d/create_fixtures.sql