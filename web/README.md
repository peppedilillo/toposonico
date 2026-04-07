# Usage

Install tippecanoe with:

```shell
git clone https://github.com/mapbox/tippecanoe.git
cd tippecanoe
make -j
make install
```

For generating the tiles run:

```shell
chmod +x build.sh
./build.sh
```

The generated tiles live in [tiles](/Users/peppedilillo/Progetti/sick/sick/web/tiles) and are served separately from the frontend.

For local development with Docker Compose:

```shell
docker compose up
```

This starts:

- frontend on `http://localhost:5173`
- backend on `http://localhost:8000`
- tiles on `http://localhost:8081/tiles/...`
