# pgdurable

A durable job queue that lives in the Postgres you already run. No Redis, no broker, one table.

```ts
import pg from 'pg'
import { Queue } from 'pgdurable'

const queue = new Queue(new pg.Pool({ connectionString: process.env.DATABASE_URL }))
await queue.migrate()

await queue.enqueue('receipts', { orderId: 1001 }, { idempotencyKey: 'receipt-1001' })

const worker = queue.work('receipts', async (job) => {
  await sendReceipt(job.payload.orderId)
})

process.on('SIGTERM', () => worker.stop())
```

## What it gives you

| | |
|---|---|
| **Retries** | A handler that throws is run again with exponential backoff and jitter, up to `maxAttempts` (default 5). The last error is kept on the job. |
| **Idempotency keys** | Enqueueing twice with the same key on the same queue creates one job. Safe against double clicks, retried requests and redelivered webhooks. |
| **Scheduled jobs** | `delayMs` or `runAt`. A job never runs early. |
| **Crash recovery** | A worker that dies mid-job loses its claim after `lockMs`; another worker picks the job up. |
| **Transactional enqueue** | Pass your open transaction as `client` and the job exists if and only if your transaction commits. |
| **Graceful shutdown** | `worker.stop()` takes no new jobs and waits for the ones in flight. |

## The guarantee, stated exactly

**At-least-once.** Every enqueued job is run until a handler finishes without throwing, or until it has been attempted `maxAttempts` times.

A job can run more than once: if a worker finishes the work and dies before recording that it did, the job is run again. So a handler must be safe to repeat. That is true of every queue that survives crashes; the ones that claim otherwise have moved the problem somewhere you cannot see it.

What pgdurable does promise is that two workers never hold the same job at the same time while both are healthy, and that a worker which lost its claim cannot overwrite the result of the worker that took over.

## How it works

Three ideas carry the whole library. `src/index.ts` is under 400 lines and reads top to bottom.

**1. `FOR UPDATE SKIP LOCKED` hands out jobs.**

```sql
with picked as (
  select id from pgdurable_jobs
   where queue = $1 and status = 'pending' and run_at <= now()
   order by run_at, id
   limit $4
   for update skip locked
)
update pgdurable_jobs j
   set status = 'running', attempts = j.attempts + 1,
       locked_by = $2, locked_until = now() + ...
  from picked where j.id = picked.id
returning j.*
```

A row that another worker is in the middle of claiming is skipped, not waited on. Workers never receive the same job and never queue up behind each other.

**2. A claim is a lease, not a lock.** Nothing is held open while your handler runs: no transaction, no connection. The row just says `locked_until`. The worker renews it every `lockMs / 2`. If the worker dies, the renewals stop, the lease runs out, and the next `claim` puts the job back.

**3. `attempts` is a fencing token.** Every write a worker makes to its job is conditional on `attempts` still being the number it was handed. Once a lease expires and someone else claims the job, `attempts` has moved on, and the late worker's `complete` matches zero rows. A paused process that wakes up cannot mark someone else's run as done.

Every time comparison uses the database's `now()`, never a worker's clock, so workers with skewed clocks still agree on what has expired.

## API

```ts
new Queue(pool)

queue.migrate()                          // create table + indexes; safe on every boot
queue.enqueue(name, payload, options?)   // -> { id, deduplicated }
queue.work(name, handler, options?)      // -> Worker
queue.get(id)                            // -> Job | null
queue.stats(name)                        // -> { pending, running, done, failed }
worker.stop()                            // drain and stop
```

**Enqueue options**

| option | default | |
|---|---|---|
| `idempotencyKey` | none | one job per key per queue |
| `delayMs` / `runAt` | now | earliest time to run |
| `maxAttempts` | 5 | total runs before the job is marked `failed` |
| `client` | none | a `PoolClient` with an open transaction |

**Worker options**

| option | default | |
|---|---|---|
| `concurrency` | 1 | jobs handled at once |
| `pollMs` | 1000 | wait between polls when the queue is empty |
| `lockMs` | 30000 | lease length; also how long a crashed worker's job waits |
| `backoff` | 1s doubling, capped at 5 min, jittered | `(attempt) => ms` |
| `onError` | `console.error` | errors from the queue itself, not your handler |

### Enqueue in the same transaction as your write

This is the reason to keep a queue in Postgres at all. There is no window where the order was saved but the job was lost, or the job ran for an order that was rolled back.

```ts
const client = await pool.connect()
try {
  await client.query('begin')
  await client.query('insert into orders (id, total) values ($1, $2)', [id, total])
  await queue.enqueue('receipts', { orderId: id }, { client })
  await client.query('commit')
} catch (err) {
  await client.query('rollback')
  throw err
} finally {
  client.release()
}
```

## Tests

The tests run against a real Postgres. Locking is the point of the library and a mock has none.

```bash
npm run test:local     # starts a throwaway cluster, runs the tests, deletes it
DATABASE_URL=... npm test
```

Each guarantee above has a test that fails when the guarantee is broken:

- 300 jobs, four workers on separate connection pools: every job runs exactly once
- 50 concurrent enqueues with one idempotency key: one job
- a claimed job whose worker never returns is picked up after the lease expires
- a worker that lost its lease cannot complete, fail or extend the job
- a handler four times longer than the lease is still run once, because of the heartbeat
- a job enqueued in a rolled-back transaction does not exist

## What it does not do yet

- **No `LISTEN/NOTIFY`.** Workers poll, so a job waits up to `pollMs` to start.
- **No recurring (cron) jobs.**
- **No cleanup.** Finished jobs stay in the table; delete old `done` rows yourself for now.
- **No dashboard.**
- **No published benchmarks.** It has not been load-tested and makes no throughput claim.

## Licence

MIT
