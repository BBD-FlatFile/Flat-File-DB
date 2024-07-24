from fastapi import FastAPI, Depends
from app.routers import transactions, database
from app.dependencies import verify_token

app = FastAPI(dependencies=[Depends(verify_token)])
app.include_router(transactions.router)
app.include_router(database.router)


@app.get("/")
async def root():
    return {"message": "Welcome to flat-file-db API!"}
