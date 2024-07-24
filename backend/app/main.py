from fastapi import FastAPI, Depends
from app.routers import transactions
from app.dependencies import verify_token

app = FastAPI(dependencies=[Depends(verify_token)])
app.include_router(transactions.router)


@app.get("/")
async def root():
    return {"message": "Welcome to flat-file-db API!"}
