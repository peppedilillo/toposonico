# Frontend

The frontend is a Vite + React app that renders the map with DeckGL.

## Local development

Copy `.env.example` to `.env.local` if you want to override the default tile origin.

```shell
cp .env.example .env.local
npm install
npm run dev
```

By default the app requests tiles from `http://localhost:8081/tiles/{z}/{x}/{y}.pbf`.

## With Docker Compose

From [web](/Users/peppedilillo/Progetti/sick/sick/web):

```shell
docker compose up
```

Compose starts a dedicated tile server, so Vite only serves the frontend.
