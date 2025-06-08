# Reducer

### Start the containers
To build the Docker image

```bash
docker build -t best-red .
```

### Run the reducer
Execute the reducer tool on the default query.sql query file

```bash
docker run --rm -v /var/run/docker.sock:/var/run/docker.sock best-red
```


Open a shell inside the container of reducer
```bash
docker run -it --rm -v /var/run/docker.sock:/var/run/docker.sock --entrypoint /bin/sh best-red
```

Main command to run the reducer (/usr/bin/reducer), either on bug or crash oracle
```bash
./reducer --query query.sql --test test-diff.sh
./reducer --query query.sql --test test-crash.sh
```