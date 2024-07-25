from fastapi import Header, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer
from typing import Annotated
import jwt


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


async def verify_token(token: Annotated[str, Depends(oauth2_scheme)]):
    try:
        payload = jwt.decode(token, options={'verify_signature': False})
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")
