from fastapi import FastAPI
from app.routers import transactions

app = FastAPI()
app.include_router(transactions.router)


@app.get("/")
async def root():
    return {"message": "Welcome to flat-file-db API!"}
