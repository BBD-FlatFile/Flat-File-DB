from fastapi import Header, HTTPException
from typing import Annotated
import jwt


async def verify_token(jwt_token: Annotated[str, Header()]):
    try:
        payload = jwt.decode(jwt_token, options={'verify_signature': False})
        print(payload)
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")
