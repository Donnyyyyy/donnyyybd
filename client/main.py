import asyncio
from random import randrange
from time import time

from donnydb import DonnydbClient


async def make_requests(n_req: int) -> tuple[int, int]:
    ok, err = 0, 0

    n_range = (10 ** (len(str(n_req)) + 1), 10 ** (len(str(n_req)) + 2) - 1)
    async with DonnydbClient() as client:
        values = {}
        for _ in range(n_req):
            n = randrange(*n_range)
            key = f'key {n}'
            value = (f'value {n}' * ((n + 1) % 100)).encode()
            values[key] = value
            try:
                await client.set(key, values[key])
                stored_value = await client.get(key)
                if stored_value == value:
                    ok += 1
                else:
                    err += 1
            except:
                err += 1

        # for key, value in values.items():
        #     try:
        #         stored_value = await client.get(key)
        #         if stored_value == value:
        #             ok += 1
        #         else:
        #             err += 1
        #     except:
        #         err += 1

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
