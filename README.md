# sqlite-testing

### Start the containers
To build the Docker images and starts both containers in the background:

```bash
docker compose up --build -d
```

### Run the generator
Execute the fuzzing tool by calling its entrypoint /usr/bin/test-db from within the best-gen container:

```bash
docker exec best-gen /usr/bin/test-db
```

### Stop and clean up

To shut down the containers and clean up
```bash
docker compose down
```
