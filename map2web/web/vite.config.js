import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [
    react(),
    {
      name: 'pbf-gzip-headers',
      configureServer(server) {
        server.middlewares.use((req, res, next) => {
          if (req.url.endsWith('.pbf')) {
            res.setHeader('Content-Encoding', 'gzip');
            res.setHeader('Content-Type', 'application/x-protobuf');
          }
          next();
        });
      },
    },
  ],
  server: {
    watch: {
      ignored: ['**/public/tiles/**'],
    },
  },
})
