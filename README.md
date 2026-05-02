# Toposonico

This monorepo contains all code used for [toposonico.com](https://toposonico.com), a navigable map and recommender for 17M discographic entities across music tracks, artists, albums and labels.

<div align="center">
<img src="screenshot_mobile.png" width="300" alt="Toposonico Mobile View">
</div>


## How it works

Toposonico is built around a skipgram [word2vec](https://en.wikipedia.org/wiki/Word2vec) model trained over ~7B playlists.
Tracks are embedded in a 128d space. 
Embeddings for albums, artists and labels are computed marginalizing over tracks.
The 2D map was built with [UMAP](https://umap-learn.readthedocs.io/en/latest/how_umap_works.html).

The model was trained in the cloud with a NVIDIA A100. UMAP was trained in the cloud too, using the fast [RAPIDS CuML](https://developer.nvidia.com/blog/even-faster-and-more-scalable-umap-on-the-gpu-with-rapids-cuml/) implementation.
The recommender indexes were built and tuned with [FAISS](https://github.com/facebookresearch/faiss).
The frontend slippy map was implemented with [MapLibre GL JS](https://maplibre.org/maplibre-gl-js/docs/) over tiles built with [tippecanoe](https://github.com/mapbox/tippecanoe).

## Project structure

The project is structured in three main piecies:

* `ml` contains everything needed to train the recommender model, export its embeddings and UMAP representation, and to build lookup tables for tracks, album, artists and labels.
* `db` implements a pipeline transforming the previous data products into a proper database and a collection of GeoJSON files.
* `web` is itself split into multiple components: a TS-React frontend living in `frontend`, a Python FastAPI backend living in `backend`, and a Martin tile server living in `tileserver`.

Each of these subprojects contains a README describing in more detail each component usage and goal.
