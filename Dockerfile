ARG PG_VERSION=16
FROM postgres:${PG_VERSION}

# Install build dependencies, pg_cron, and pgTAP
RUN apt-get update \
    && apt-get install -y \
        postgresql-server-dev-${PG_MAJOR} \
        build-essential \
        git \
    # Install pg_cron
    && git clone https://github.com/citusdata/pg_cron.git \
    && cd pg_cron \
    && make && make install \
    && cd .. && rm -rf pg_cron \
    # Install pgTAP
    && git clone https://github.com/theory/pgtap.git \
    && cd pgtap \
    && make && make install \
    && cd .. && rm -rf pgtap \
    # Install pg_prove for running tests
    && apt-get install -y libtap-parser-sourcehandler-pgtap-perl \
    # Clean up
    && apt-get remove -y build-essential git \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# pg_cron requires these settings
RUN echo "shared_preload_libraries = 'pg_cron'" >> /usr/share/postgresql/postgresql.conf.sample \
    && echo "cron.database_name = 'postgres'" >> /usr/share/postgresql/postgresql.conf.sample
