# Reducer

### Start the containers
To build the Docker image

```bash
docker build -t best-red .
```

### Run the generator
Execute the reducer tool on the current original_test.sql query file

```bash
docker run --rm -v /var/run/docker.sock:/var/run/docker.sock best-red
```

