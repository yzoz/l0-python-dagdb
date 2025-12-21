from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def read_root():
    from fastapi.responses import FileResponse
    return FileResponse("static/index.html")

@router.get("/ru")
async def read_root():
    from fastapi.responses import FileResponse
    return FileResponse("static/ru/index.html")

@router.get("/whitepaper.pdf")
async def read_root():
    from fastapi.responses import FileResponse
    return FileResponse("static/whitepaper.pdf")

@router.get("/whitepaper.docx")
async def read_root():
    from fastapi.responses import FileResponse
    return FileResponse("static/whitepaper.docx")