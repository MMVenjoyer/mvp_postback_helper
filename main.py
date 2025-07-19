from fastapi import FastAPI
from postback_router import router as postback_router
from resolver_router import router as resolver_router

app = FastAPI()
app.include_router(postback_router, prefix="/postback")
app.include_router(resolver_router, prefix="/resolve")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
