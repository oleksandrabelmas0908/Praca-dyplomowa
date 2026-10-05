import shutil
from uuid import uuid4

from fastapi import APIRouter, HTTPException, UploadFile

from catalog.schemas import ImageUploadResponse
from catalog.tasks import IMAGE_DIR, process_product_image
from shared.events import current_correlation_id
from shared.middleware import HEADER

CONTENT_TYPES = ("image/jpeg", "image/png", "image/webp")
MAX_IMAGE_BYTES = 10 * 1024 * 1024

router = APIRouter()


@router.post("/products/{product_id}/image", status_code=202)
def upload_image(product_id: int, file: UploadFile) -> ImageUploadResponse:
    if file.content_type not in CONTENT_TYPES:
        raise HTTPException(
            status_code=415,
            detail=f"Content type {file.content_type} is not one of {', '.join(CONTENT_TYPES)}",
        )
    if file.size is None or file.size > MAX_IMAGE_BYTES:
        raise HTTPException(status_code=413, detail=f"Image is larger than {MAX_IMAGE_BYTES} bytes")

    directory = IMAGE_DIR / str(product_id)
    directory.mkdir(parents=True, exist_ok=True)
    source = directory / "original"
    temporary = directory / f"original.{uuid4()}"
    with temporary.open("wb") as target:
        shutil.copyfileobj(file.file, target)
    temporary.replace(source)

    process_product_image.apply_async(
        (product_id, str(source)), headers={HEADER: str(current_correlation_id())}
    )
    return ImageUploadResponse(product_id=product_id, status="queued")
