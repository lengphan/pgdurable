import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { after, afterEach, before, test } from 'node:test'
import pg from 'pg'
import { Queue, type Handler, type Worker, type WorkerOptions } from '../src/index.js'

// These run against a real Postgres. Locking behaviour is the whole point of
// the library and a mock cannot have any.
const url = process.env.DATABASE_URL
if (!url) throw new Error('DATABASE_URL is not set. Run: npm run test:local')

const pool = new pg.Pool({ connectionString: url, max: 20 })
const queue = new Queue(pool)

// One table is shared by every test, so each test works on its own queue name.
const fresh = () => `q-${randomUUID()}`
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

async function waitFor(check: () => Promise<boolean> | boolean, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await check()) return
    await sleep(20)
  }
  throw new Error(`condition not met within ${timeoutMs}ms`)
}

// A failed assertion must not leave a worker polling, or the run never exits.
const started: Worker<never>[] = []
const extraPools: pg.Pool[] = []

function work<T = unknown>(q: Queue, name: string, handler: Handler<T>, opts?: WorkerOptions): Worker<T> {
  const worker = q.work<T>(name, handler, opts)
  started.push(worker as unknown as Worker<never>)
  return worker
}

afterEach(async () => {
  await Promise.all(started.splice(0).map((w) => w.stop()))
  await Promise.all(extraPools.splice(0).filter((p) => !p.ended).map((p) => p.end()))
})

before(async () => {
  // Raced on purpose: migrate() claims to be safe from many processes at once.
  await Promise.all([queue.migrate(), queue.migrate(), queue.migrate()])
})

after(async () => {
  await pool.end()
})

test('a job is handed to the handler with its payload, then marked done', async () => {
  const name = fresh()
  const seen: unknown[] = []
  const { id } = await queue.enqueue(name, { to: 'a@example.com' })

  const worker = work(queue, name, (job) => void seen.push(job.payload), { pollMs: 20 })
  await waitFor(async () => (await queue.get(id))?.status === 'done')
  await worker.stop()

  assert.deepEqual(seen, [{ to: 'a@example.com' }])
})

test('the same idempotency key enqueues once', async () => {
  const name = fresh()
  const first = await queue.enqueue(name, { n: 1 }, { idempotencyKey: 'charge-42' })
  const second = await queue.enqueue(name, { n: 2 }, { idempotencyKey: 'charge-42' })

  assert.equal(first.deduplicated, false)
  assert.equal(second.deduplicated, true)
  assert.equal(second.id, first.id)
  assert.equal((await queue.stats(name)).pending, 1)

  // The key is scoped to its queue.
  const elsewhere = await queue.enqueue(fresh(), {}, { idempotencyKey: 'charge-42' })
  assert.equal(elsewhere.deduplicated, false)
})

test('fifty concurrent enqueues with one key still produce one job', async () => {
  const name = fresh()
  const results = await Promise.all(
    Array.from({ length: 50 }, () => queue.enqueue(name, {}, { idempotencyKey: 'only-once' })),
  )
  assert.equal(results.filter((r) => !r.deduplicated).length, 1)
  assert.equal(new Set(results.map((r) => r.id)).size, 1)
  assert.equal((await queue.stats(name)).pending, 1)
})

test('a failing job is retried and succeeds on a later attempt', async () => {
  const name = fresh()
  const attempts: number[] = []
  const { id } = await queue.enqueue(name, {})

  const worker = work(
    queue,
    name,
    (job) => {
      attempts.push(job.attempts)
      if (job.attempts < 3) throw new Error('not yet')
    },
    { pollMs: 20, backoff: () => 10 },
  )
  await waitFor(async () => (await queue.get(id))?.status === 'done')
  await worker.stop()

  assert.deepEqual(attempts, [1, 2, 3])
})

test('a job that keeps failing stops at maxAttempts and keeps its error', async () => {
  const name = fresh()
  let runs = 0
  const { id } = await queue.enqueue(name, {}, { maxAttempts: 3 })

  const worker = work(
    queue,
    name,
    () => {
      runs++
      throw new Error('card declined')
    },
    { pollMs: 20, backoff: () => 10 },
  )
  await waitFor(async () => (await queue.get(id))?.status === 'failed')
  await sleep(150) // long enough for a fourth run to have happened if it was going to
  await worker.stop()

  const job = await queue.get(id)
  assert.equal(runs, 3)
  assert.equal(job?.attempts, 3)
  assert.match(job?.lastError ?? '', /card declined/)
})

test('a delayed job does not run early', async () => {
  const name = fresh()
  let ranAt = 0
  const enqueuedAt = Date.now()
  const { id } = await queue.enqueue(name, {}, { delayMs: 400 })

  const worker = work(queue, name, () => void (ranAt = Date.now()), { pollMs: 20 })
  await sleep(200)
  assert.equal((await queue.get(id))?.status, 'pending')

  await waitFor(async () => (await queue.get(id))?.status === 'done')
  await worker.stop()
  assert.ok(ranAt - enqueuedAt >= 380, `ran after ${ranAt - enqueuedAt}ms`)
})

test('four workers on separate connections run 300 jobs exactly once each', async () => {
  const name = fresh()
  const total = 300
  for (let i = 0; i < total; i++) await queue.enqueue(name, { i })

  // Separate pools stand in for separate processes.
  const pools = Array.from({ length: 4 }, () => new pg.Pool({ connectionString: url, max: 5 }))
  extraPools.push(...pools)
  const runs = new Map<number, number>()
  const workers = pools.map((p) =>
    work<{ i: number }>(
      new Queue(p),
      name,
      async (job) => {
        runs.set(job.payload.i, (runs.get(job.payload.i) ?? 0) + 1)
        await sleep(1)
      },
      { pollMs: 20, concurrency: 5 },
    ),
  )

  await waitFor(async () => (await queue.stats(name)).done === total, 20_000)
  await Promise.all(workers.map((w) => w.stop()))
  await Promise.all(pools.map((p) => p.end()))

  assert.equal(runs.size, total)
  assert.ok([...runs.values()].every((n) => n === 1), 'a job ran more than once')
})

test('a job whose worker died is picked up again when the claim expires', async () => {
  const name = fresh()
  const { id } = await queue.enqueue(name, {})

  // Claim it and walk away, as a crashed process would.
  const [orphan] = await queue.claim(name, 'worker-that-dies', 150, 1)
  assert.equal(orphan?.id, id)

  const attempts: number[] = []
  const worker = work(queue, name, (job) => void attempts.push(job.attempts), { pollMs: 20 })
  await waitFor(async () => (await queue.get(id))?.status === 'done')
  await worker.stop()

  assert.deepEqual(attempts, [2])
})

test('a worker that lost its claim cannot overwrite the worker that took over', async () => {
  const name = fresh()
  await queue.enqueue(name, {})

  const [slow] = await queue.claim(name, 'slow', 100, 1)
  assert.ok(slow)
  await sleep(250)
  const [taker] = await queue.claim(name, 'taker', 5000, 1)
  assert.ok(taker)
  assert.equal(taker.id, slow.id)
  assert.equal(taker.attempts, 2)

  // The slow worker wakes up and tries everything it could try.
  assert.equal(await queue.complete(slow, 'slow'), false)
  assert.equal(await queue.fail(slow, 'slow', new Error('late'), 0), false)
  assert.equal(await queue.extend(slow, 'slow', 5000), false)
  assert.equal((await queue.get(slow.id))?.status, 'running')

  assert.equal(await queue.complete(taker, 'taker'), true)
  assert.equal((await queue.get(slow.id))?.status, 'done')
})

test('the heartbeat keeps a long job from being handed to a second worker', async () => {
  const name = fresh()
  let runs = 0
  const { id } = await queue.enqueue(name, {})
  const opts = { pollMs: 20, lockMs: 200 }

  // The handler takes four times the claim length. Without renewal the
  // second worker would take the job partway through.
  const handler = async () => {
    runs++
    await sleep(800)
  }
  const a = work(queue, name, handler, opts)
  const b = work(queue, name, handler, opts)
  await waitFor(async () => (await queue.get(id))?.status === 'done')
  await Promise.all([a.stop(), b.stop()])

  assert.equal(runs, 1)
  assert.equal((await queue.get(id))?.attempts, 1)
})

test('a job enqueued inside a transaction exists only if the transaction commits', async () => {
  const name = fresh()

  const rolledBack = await pool.connect()
  await rolledBack.query('begin')
  await queue.enqueue(name, { kind: 'rolled back' }, { client: rolledBack })
  await rolledBack.query('rollback')
  rolledBack.release()
  assert.equal((await queue.stats(name)).pending, 0)

  const committed = await pool.connect()
  await committed.query('begin')
  await queue.enqueue(name, { kind: 'committed' }, { client: committed })
  await committed.query('commit')
  committed.release()
  assert.equal((await queue.stats(name)).pending, 1)
})

test('stop() waits for the job in flight', async () => {
  const name = fresh()
  let finished = false
  const { id } = await queue.enqueue(name, {})

  const worker = work(
    queue,
    name,
    async () => {
      await sleep(300)
      finished = true
    },
    { pollMs: 20 },
  )
  await waitFor(async () => (await queue.get(id))?.status === 'running')
  await worker.stop()

  assert.equal(finished, true)
  assert.equal((await queue.get(id))?.status, 'done')
})
