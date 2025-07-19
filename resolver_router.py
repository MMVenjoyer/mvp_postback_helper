from fastapi import APIRouter, Query
import httpx
import re
from urllib.parse import urlparse, parse_qs

router = APIRouter()


@router.get("/uuid")
async def resolve_uuid(url: str = Query(...)):
    try:
        uuid = await resolve_uuid_from_url(url)
        return {"status": "ok", "uuid": uuid}
    except Exception as e:
        return {"status": "error", "error": str(e)}


async def resolve_uuid_from_url(url: str, max_redirects: int = 10) -> str:
    current_url = url
    visited = set()

    async with httpx.AsyncClient(
        follow_redirects=False,
        timeout=httpx.Timeout(10.0),
        headers={"User-Agent": "Mozilla/5.0"}
    ) as client:
        for _ in range(max_redirects):
            if current_url in visited:
                raise ValueError("♻️ Зацикливание на редиректе")
            visited.add(current_url)

            response = await client.get(current_url)
            location = response.headers.get("location")
            status = response.status_code

            if status in {301, 302} and not location:
                raise ValueError("⚠️ Редирект без location")
            if not location:
                break

            next_url = (
                location if location.startswith("http") or location.startswith("tg://")
                else f"{urlparse(current_url).scheme}://{urlparse(current_url).netloc}{location}"
            )

            if next_url.startswith("tg://"):
                return extract_uuid_from_deep_link(next_url)

            current_url = next_url

    raise ValueError("❌ UUID не найден: диплинк не был обнаружен")


def extract_uuid_from_deep_link(deep_link: str) -> str:
    parsed = urlparse(deep_link)
    params = parse_qs(parsed.query)
    uuid_list = params.get("start")
    if uuid_list and re.match(r"^[0-9a-fA-F\-]{36}$", uuid_list[0]):
        return uuid_list[0]
    raise ValueError(f'💥 UUID не найден в диплинке: {deep_link}')
