import asyncio

from donnydb import DonnydbClient


async def main():
    async with DonnydbClient() as client:
        for i in range(10):
            await client.set(f'key {i}', (f'value {i}' * (i + 1)).encode())

        for i in range(100):
            v = await client.get(f'key {i}')
            print(f'for "key {i}" value is {len(v)} bytes long')


if __name__ == '__main__':
    asyncio.new_event_loop().run_until_complete(main())
