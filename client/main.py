import asyncio
from time import time

from donnydb import DonnydbClient


async def make_requests(n_req: int) -> tuple[int, int]:
    ok, err = 0, 0

    async with DonnydbClient() as client:
        for i in range(n_req):
            key = f'key {i}'
            value = (f'value {i}' * ((i + 1) % 100)).encode()
            try:
                await client.set(key, value)
                stored_value = await client.get(key)
                if stored_value == value:
                    ok += 1
                else:
                    err += 1
            except:
                err += 1

        return ok, err


async def main():
    clients = 10
    req_total = 10000

    s = time()
    results = await asyncio.gather(*[make_requests(req_total // clients) for _ in range(clients)])
    t_time = time() - s
    t_ok = sum(ok for ok, *_ in results)
    t_err = sum(err for _, err, *_ in results)
    print(
        f'made {req_total} requests with {clients} clients in {t_time:.2f}s, ok: {t_ok}, err: {t_err}'
    )


if __name__ == '__main__':
    asyncio.new_event_loop().run_until_complete(main())
