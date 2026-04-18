from fastapi import APIRouter
from fastapi import HTTPException

from src.shared import check_config
from src.shared import get_db
from src.shared import get_faiss_indexes
from src.shared import get_meili_index

router = APIRouter()


def ready_check() -> None:
    check_config()

    db = get_db()
    db.execute("SELECT 1").fetchone()

    indexes = get_faiss_indexes()
    for index in indexes:
        if index.ntotal <= 0:
            raise RuntimeError("FAISS index is empty")

    get_meili_index().get_stats()


@router.get("/api/ready")
async def ready():
    try:
        ready_check()
    except Exception as e:
        raise HTTPException(status_code=503, detail="Readiness check failed") from e
    return {"status": "ok"}
