from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.info import router as info_router
from src.repr import router as repr_router
from src.recommend import router as recommend_router
from src.search import router as search_router

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["GET"],
)

app.include_router(search_router)
app.include_router(info_router)
app.include_router(recommend_router)
app.include_router(repr_router)


@app.get("/")
async def root():
    return {"message": "API is running"}
