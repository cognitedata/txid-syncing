#!/usr/bin/env bash
set -e # (errexit): exit with nonzero exit code if anything fails
# set -x # (trace): print commands before they are executed
set -o pipefail

source .env



docker stop postgres elasticsearch || true


postgres(){
    passopt=()
for var in \
    PGPASSWORD DB_HOST DB_PORT
do
    if [ -n "${!var}" ]; then
       passopt+=(-e $var)
    fi
done

docker run \
 --rm \
 --name=postgres \
 -w "$(pwd)" \
 -v "$(pwd):/$(pwd)" \
 -e POSTGRES_PASSWORD=password \
 -e POSTGRES_USER=postgres \
 --add-host host.docker.internal:host-gateway \
 --net host \
 "${passopt[@]}" \
postgres:${PG_VERSION}
}

elasticsearch(){
    passopt=()
for var in \
    PGPASSWORD DB_HOST DB_PORT
do
    if [ -n "${!var}" ]; then
       passopt+=(-e $var)
    fi
done

docker run \
 --rm \
 --name=elasticsearch \
 -w "$(pwd)" \
 -v "$(pwd):/$(pwd)" \
 --add-host host.docker.internal:host-gateway \
 --net host \
 "${passopt[@]}" \
-p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" \
elasticsearch:${ES_VERSION}
}

retry() {
  local max_attempts="${1}"
  shift
  local retry_delay_seconds="${1}"
  shift
  local cmd="${@}"
  local attempt_num=1

  until ${cmd}; do
    ((attempt_num >= max_attempts)) && {
      echo "Attempt ${attempt_num} failed and there are no more attempts left!"
      return 1
    }
    echo "Attempt ${attempt_num} failed! Trying again in ${retry_delay_seconds} seconds..."
    attempt_num=$((attempt_num + 1))
    sleep ${retry_delay_seconds}
  done
}

postgres &
elasticsearch &

retry 1>&2 ${MAX_ATTEMPTS:-50} ${RETRY_DELAY_SECONDS:-1} \
docker exec \
postgres \
pg_isready \
-U postgres \
-d postgres

psql -vON_ERROR_STOP=on ${POSTGRES_CONFIG} -f ./apply-schema.sql

docker logs -f postgres    # this follows until a ctrl+c
docker logs elasticsearch  # without the -f it just dumps it at the end
docker stop postgres elasticsearch || true
