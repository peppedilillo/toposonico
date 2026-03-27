import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
    plugins: [
        tailwindcss(),
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
        proxy: {
            '/api': 'http://localhost:8000',
        },
    },
})
