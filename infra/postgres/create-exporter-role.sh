#!/bin/sh
set -e
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v password="$POSTGRES_EXPORTER_PASSWORD" <<'SQL'
CREATE ROLE exporter LOGIN PASSWORD :'password';
GRANT pg_monitor TO exporter;
SQL
