# Sick

Welcome!

This monorepo contains all code used for _sick_, a recommender and navigable map of ~17M entities between tracks, albums, artists and labels. The project is structured in three main sections:

* `ml` contains all things necessary to train the recommender model, export its embeddings and UMAP representation, and to build lookup tables.
* `db` implements a pipeline transforming the previous data products into a proper database and a collection of GEOJson.
* `web` is itself split into multiple components: a TS+React frontend living in `frontend`, a FastAPI backend living in `backend`, and tile server living in `tileserver`. How imaginative. 

Each of these project host a number of README describing in more detail each component usage and goal. 
