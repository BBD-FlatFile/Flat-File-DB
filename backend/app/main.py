from fastapi import FastAPI
from app.routers import transactions, database

app = FastAPI()
app.include_router(transactions.router)
app.include_router(database.router)


@app.get("/")
async def root():
    return {"message": "Welcome to flat-file-db API!"}
