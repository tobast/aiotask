# aiotask

A remote, non-distributed, simple asynchronous task queue in Python

## Architecture

Aiotask provides a simple client/server task queue architecture, based on
[`asyncio`](https://docs.python.org/3/library/asyncio.html).

The server part ("worker") listens on the network. A client can connect to it
and submit it *tasks*, consisting in any kind of data in the form of Python
`bytes`. A user-defined function is called on these bytes, and should return an
result, which is in turn returned to the client.

The asyncio approach allows for designs such as:

```python
async def bar(data):
  task = make_task_of_data(data)
  return await task.run()

async def foo(data):
  preprocess(data)
  bar(data)
  postprocess(data)

for data in large_data_array:
  asyncio.create_task(foo(data))
```

which allows to run some parts of a program on a remote machine, without
breaking the control flow. The program doesn't *block* until the remote tasks
are complete: `postprocess` for the third element can eg. be run while the
tenth element is run on the worker.


## Security considerations

**Transport security.** The protocol implemented in aiotask is as simple as it
gets. Specifically, it doesn't include **any** security, privacy or
authentication features. The recommended way to use aiotask is either on a
trusted, local network (correctly firewalled), eg. between different trusted
virtual machines/containers; or using any kind of network tunnel, for instance
a VPN. This second option, if properly configured, should cover all these
needs.

**Execution security.** The submitted "tasks" are solely data, the code
executed on the worker end is completely left to the user. The code called on
the tasks' data is not sandboxed in any way.

## Protocol

The protocol is
[TLV](https://en.wikipedia.org/wiki/Type%E2%80%93length%E2%80%93value)-based.
The various TLVs are explicited in `aiotask.constants`.

## Example code

See `test/client.py` and `test/server.py` for examples.

## Executors

The executor is the part of the worker that actually executes the tasks. The
main executor is `aiotask.executor.ProcessPoolExecutor`, running the tasks in a
[`multiprocessing.pool.Pool`](https://docs.python.org/3/library/multiprocessing.html#multiprocessing.pool.Pool).
It is possible to define a custom executor by inheriting from
`aiotask.executor.BaseExecutor` or a child class.
