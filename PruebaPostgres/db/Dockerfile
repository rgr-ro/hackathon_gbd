FROM postgres:16

# Install build tools and PostgreSQL server dev headers, build pgvector and install it
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
     build-essential \
     ca-certificates \
     git \
     postgresql-server-dev-16 \
  && git clone --depth 1 https://github.com/pgvector/pgvector.git /tmp/pgvector \
  && cd /tmp/pgvector \
  && make \
  && make install \
  && cd / \
  && rm -rf /tmp/pgvector \
  && apt-get remove -y git build-essential \
  && apt-get autoremove -y \
  && rm -rf /var/lib/apt/lists/*
