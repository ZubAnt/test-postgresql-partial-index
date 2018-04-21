import asyncio
import asyncpg
import logging
from asyncio import get_event_loop, sleep, AbstractEventLoop, Queue, QueueEmpty
from typing import Any, Dict, List
from uuid import uuid4

import os
import uvloop
from asyncpg import Connection


class SchedulerEmptyQueue(Exception):
    pass


class Flags(object):

    def __init__(self, is_waiting: bool) -> None:
        self._is_waiting = is_waiting

    @property
    def is_waiting(self) -> bool:
        return self._is_waiting

    @is_waiting.setter
    def is_waiting(self, value):
        self._is_waiting = value


class Stub(object):

    @staticmethod
    def get_instance() -> Dict[str, Any]:
        return {
            'field_id': uuid4(),
            'title': uuid4().hex
        }

    @staticmethod
    def get_array_of_instance(size: int) -> List[Dict[str, Any]]:
        return [{
            'field_id': uuid4(),
            'title': uuid4().hex
        } for _ in range(size)]


class Scheduler(object):

    def __init__(self, p_queue: Queue, size: int, flags: Flags) -> None:
        self._queue = p_queue
        self._is_running = False
        self._size = size
        self._flags = flags

    @property
    def is_running(self) -> bool:
        return self._is_running

    async def start(self) -> None:
        logging.info(f"[Scheduler] start")
        self._is_running = True
        while self.is_running:

            data = Stub.get_instance()
            self._decrement(1)
            if self._size <= 0:
                logging.info(f"[Scheduler] empty tasks")
                self._is_running = False
                self._flags.is_waiting = False
            logging.debug(f"[Scheduler] put data: {data}")
            await self._queue.put(data)
            await sleep(0)

    def _decrement(self, n: int) -> None:
        self._size -= n

    def stop(self) -> None:
        logging.info(f"[Scheduler] stop")
        self._is_running = False

    def spawn(self):
        return self.start()


class AsyncPostgresDataContext(object):

    @property
    def conn_string(self) -> str:
        return f'postgres://{self._user}:{self._password}@{self._host}:{self._port}/{self._database}'

    def __init__(self, host: str, port: int, database: str, user: str, password: str, p_loop: AbstractEventLoop) -> None:
        self._host = host
        self._port = port
        self._database = database
        self._user = user
        self._password = password
        self._counter = 0
        self._step = 50000
        self._step_counter = 0
        self._pool = p_loop.run_until_complete(asyncpg.create_pool(user=user,
                                                                   password=password,
                                                                   database=database,
                                                                   host=host,
                                                                   port=port,
                                                                   loop=p_loop,
                                                                   min_size=15,
                                                                   max_size=15))

    async def callproc(self, cmd: str, *args) -> List[Dict[str, Any]]:
        conn = await self.create_connection()
        try:
            return await conn.fetch(f'SELECT * FROM {cmd};', *args)
        finally:
            await conn.close()

    async def insert(self, data: Dict[str, Any]) -> None:
        async with self._pool.acquire() as con:
            await con.execute(f"INSERT INTO test_table (field_id, title) VALUES ($1, $2)",
                              data['field_id'], data['title'])
            self._counter += 1
            if self._counter > self._step:
                self._step_counter += 1
                logging.info(f"[Filler] fill {self._step_counter * self._step}")
                self._counter = 0

    async def select(self) -> None:
        async with self._pool.acquire() as con:
            logging.info(f"select")
            await con.fetch('SELECT 1')

    async def create_connection(self) -> Connection:
        return await asyncpg.connect(self.conn_string)


class EnvAsyncPostgresDataContextFactory(object):

    @staticmethod
    def create(p_loop: AbstractEventLoop) -> AsyncPostgresDataContext:

        host = os.environ['DB_HOST']
        port = os.environ['DB_PORT']
        database = os.environ['DB_NAME']
        user = os.environ['DB_USER']
        password = os.environ['DB_PASS']

        return AsyncPostgresDataContext(host, port, database, user, password, p_loop)


class Filler(object):

    def __init__(self, p_queue: Queue, p_loop: AbstractEventLoop, flags: Flags) -> None:
        self._flags = flags
        self._queue = p_queue
        self._is_running = False
        self._context = EnvAsyncPostgresDataContextFactory.create(p_loop=p_loop)

    @property
    def is_running(self) -> bool:
        return self._is_running

    async def start(self, idx: int) -> None:
        logging.info(f"[Filler] [{idx}] start")
        self._is_running = True
        while self.is_running:

            if self._flags.is_waiting is True:
                data = await self._queue.get()
                await self._context.insert(data=data)
            else:
                if self._queue.empty() is True:
                    break
                data = await self._queue.get()
                await self._context.insert(data=data)

    def stop(self) -> None:
        logging.info(f"[Filler] stop")
        self._is_running = False

    def spawn(self, size: int):
        spawn_tasks = []
        for idx in range(size):
            spawn_tasks.append(self.start(idx=idx))
        return asyncio.gather(*spawn_tasks)


if __name__ == "__main__":

    logging.getLogger().setLevel(logging.INFO)

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = get_event_loop()

    flags = Flags(is_waiting=True)

    queue = Queue()
    scheduler = Scheduler(p_queue=queue, size=1000000, flags=flags)
    filler = Filler(p_queue=queue, p_loop=loop, flags=flags)

    scheduler_task = scheduler.spawn()
    filler_tasks = filler.spawn(size=10)

    tasks = asyncio.gather(scheduler_task, filler_tasks)

    try:
        logging.info(f"[main] start")
        loop.run_until_complete(tasks)
        asyncio.wait_for()
    except TypeError:
        logging.info(f"[main] TypeError")
    except KeyboardInterrupt:
        logging.info(f"[main] stop")
        scheduler.stop()
        filler.stop()
