# sqlite-testing

### Start the containers

```bash
docker compose up --build -d
```

### Run the generator

```bash
docker exec -it best-gen python main.py
```

### Stop and clean up

```bash
docker compose down
```
