import { randomUUID } from 'node:crypto'
import type { Pool, PoolClient } from 'pg'

export type JobStatus = 'pending' | 'running' | 'done' | 'failed'

export interface Job<T = unknown> {
  /** bigint in Postgres, so a string here: node-pg does not turn int8 into a number. */
  id: string
  queue: string
  payload: T
  status: JobStatus
  /** How many times this job has been claimed, including the current run. */
  attempts: number
  maxAttempts: number
  idempotencyKey: string | null
  runAt: Date
  lastError: string | null
  createdAt: Date
}

export interface EnqueueOptions {
  /** A second enqueue with the same key on the same queue returns the first job instead of adding one. */
  idempotencyKey?: string
  /** Do not run before this time. */
  runAt?: Date
  /** Shorthand for runAt = now + delayMs, measured on the database clock. */
  delayMs?: number
  /** Total runs allowed before the job is marked failed. Default 5. */
  maxAttempts?: number
  /**
   * Enqueue inside a transaction you already have open. The job then exists
   * if and only if your transaction commits.
   */
  client?: PoolClient
}

export interface EnqueueResult {
  id: string
  /** True when the idempotency key matched an existing job and nothing was inserted. */
  deduplicated: boolean
}

export interface WorkerOptions {
  /** Jobs handled at once by this worker. Default 1. */
  concurrency?: number
  /** How long to wait before asking again when the queue was empty. Default 1000. */
  pollMs?: number
  /**
   * How long a claim lasts without a heartbeat. The worker renews it every
   * lockMs / 2 while the handler runs, so this is the time a job waits to be
   * picked up again after its worker dies. Default 30000.
   */
  lockMs?: number
  /** Milliseconds to wait before retry number `attempt`. Default: exponential from 1s, capped at 5 minutes, with jitter. */
  backoff?: (attempt: number) => number
  /** Errors from the queue itself (not from your handler). Default: console.error. */
  onError?: (err: unknown) => void
}

export type Handler<T> = (job: Job<T>) => Promise<void> | void

const TABLE = 'pgdurable_jobs'

const COLUMNS = `id, queue, payload, status, attempts, max_attempts, idempotency_key,
  run_at, last_error, created_at`

const MIGRATION = `
create table if not exists ${TABLE} (
  id              bigserial primary key,
  queue           text        not null,
  payload         jsonb       not null default '{}'::jsonb,
  idempotency_key text,
  status          text        not null default 'pending'
                  check (status in ('pending', 'running', 'done', 'failed')),
  attempts        integer     not null default 0,
  max_attempts    integer     not null default 5,
  run_at          timestamptz not null default now(),
  locked_until    timestamptz,
  locked_by       text,
  last_error      text,
  created_at      timestamptz not null default now(),
  finished_at     timestamptz
);
create unique index if not exists ${TABLE}_idempotency
  on ${TABLE} (queue, idempotency_key) where idempotency_key is not null;
create index if not exists ${TABLE}_ready
  on ${TABLE} (queue, run_at, id) where status = 'pending';
create index if not exists ${TABLE}_running
  on ${TABLE} (queue, locked_until) where status = 'running';
`

interface Row {
  id: string
  queue: string
  payload: unknown
  status: JobStatus
  attempts: number
  max_attempts: number
  idempotency_key: string | null
  run_at: Date
  last_error: string | null
  created_at: Date
}

function toJob<T>(r: Row): Job<T> {
  return {
    id: r.id,
    queue: r.queue,
    payload: r.payload as T,
    status: r.status,
    attempts: r.attempts,
    maxAttempts: r.max_attempts,
    idempotencyKey: r.idempotency_key,
    runAt: r.run_at,
    lastError: r.last_error,
    createdAt: r.created_at,
  }
}

export function defaultBackoff(attempt: number): number {
  const ceiling = Math.min(5 * 60_000, 1000 * 2 ** (attempt - 1))
  // Jitter, so a batch that failed together does not retry together.
  return Math.round(ceiling * (0.5 + Math.random() * 0.5))
}

export class Queue {
  constructor(private readonly pool: Pool) {}

  /** Creates the table and indexes. Safe to call on every boot and from many processes at once. */
  async migrate(): Promise<void> {
    const client = await this.pool.connect()
    try {
      await client.query('begin')
      // "create table if not exists" can still collide when two processes
      // race it, so the migration runs under an advisory lock.
      await client.query(`select pg_advisory_xact_lock(hashtext('${TABLE}_migrate'))`)
      await client.query(MIGRATION)
      await client.query('commit')
    } catch (err) {
      await client.query('rollback').catch(() => {})
      throw err
    } finally {
      client.release()
    }
  }

  async enqueue<T>(queue: string, payload: T, opts: EnqueueOptions = {}): Promise<EnqueueResult> {
    const db = opts.client ?? this.pool
    const inserted = await db.query<{ id: string }>(
      `insert into ${TABLE} (queue, payload, idempotency_key, max_attempts, run_at)
       values ($1, $2::jsonb, $3, $4,
               coalesce($5::timestamptz, now() + ($6::double precision * interval '1 millisecond')))
       on conflict (queue, idempotency_key) where idempotency_key is not null do nothing
       returning id`,
      [
        queue,
        JSON.stringify(payload ?? {}),
        opts.idempotencyKey ?? null,
        opts.maxAttempts ?? 5,
        opts.runAt ?? null,
        opts.delayMs ?? 0,
      ],
    )
    const row = inserted.rows[0]
    if (row) return { id: row.id, deduplicated: false }

    const existing = await db.query<{ id: string }>(
      `select id from ${TABLE} where queue = $1 and idempotency_key = $2`,
      [queue, opts.idempotencyKey],
    )
    const found = existing.rows[0]
    if (!found) throw new Error('pgdurable: enqueue conflicted but the existing job was not found')
    return { id: found.id, deduplicated: true }
  }

  /** Starts a worker on this queue. It keeps running until you call stop(). */
  work<T = unknown>(queue: string, handler: Handler<T>, opts: WorkerOptions = {}): Worker<T> {
    const worker = new Worker<T>(this, queue, handler, opts)
    worker.start()
    return worker
  }

  async get<T = unknown>(id: string): Promise<Job<T> | null> {
    const res = await this.pool.query<Row>(`select ${COLUMNS} from ${TABLE} where id = $1`, [id])
    const row = res.rows[0]
    return row ? toJob<T>(row) : null
  }

  async stats(queue: string): Promise<Record<JobStatus, number>> {
    const res = await this.pool.query<{ status: JobStatus; n: string }>(
      `select status, count(*) as n from ${TABLE} where queue = $1 group by status`,
      [queue],
    )
    const out: Record<JobStatus, number> = { pending: 0, running: 0, done: 0, failed: 0 }
    for (const r of res.rows) out[r.status] = Number(r.n)
    return out
  }

  // ── Low-level steps. Worker is built from these four; they are public so the
  //    guarantees can be tested one at a time. ──────────────────────────────

  /**
   * Hands up to `limit` runnable jobs to `workerId`.
   *
   * Every time comparison uses the database's now(), never the worker's
   * clock, so workers with skewed clocks still agree on what has expired.
   */
  async claim<T = unknown>(queue: string, workerId: string, lockMs: number, limit: number): Promise<Job<T>[]> {
    // A job whose claim ran out belonged to a worker that died or stalled.
    // Put it back, or fail it if that was its last allowed attempt.
    await this.pool.query(
      `update ${TABLE}
          set status       = case when attempts >= max_attempts then 'failed' else 'pending' end,
              finished_at  = case when attempts >= max_attempts then now() end,
              last_error   = 'claim expired before the worker finished',
              locked_until = null,
              locked_by    = null
        where queue = $1 and status = 'running' and locked_until < now()`,
      [queue],
    )

    // SKIP LOCKED is what lets many workers share one table: a row another
    // transaction is claiming is passed over rather than waited on, so two
    // workers never receive the same job and never queue up behind each other.
    const res = await this.pool.query<Row>(
      `with picked as (
         select id from ${TABLE}
          where queue = $1 and status = 'pending' and run_at <= now()
          order by run_at, id
          limit $4
          for update skip locked
       )
       update ${TABLE} j
          set status       = 'running',
              attempts     = j.attempts + 1,
              locked_by    = $2,
              locked_until = now() + ($3::double precision * interval '1 millisecond')
         from picked
        where j.id = picked.id
       returning j.*`,
      [queue, workerId, lockMs, limit],
    )
    return res.rows.map((r) => toJob<T>(r)).sort((a, b) => Number(BigInt(a.id) - BigInt(b.id)))
  }

  /**
   * The three calls below only touch the row while this worker still holds
   * THIS attempt. `attempts` is the fencing token: once a claim expires and
   * someone else takes the job, attempts has moved on and the late worker's
   * write matches nothing. Each returns whether it applied.
   */
  async extend(job: Job, workerId: string, lockMs: number): Promise<boolean> {
    const res = await this.pool.query(
      `update ${TABLE}
          set locked_until = now() + ($4::double precision * interval '1 millisecond')
        where id = $1 and status = 'running' and locked_by = $2 and attempts = $3`,
      [job.id, workerId, job.attempts, lockMs],
    )
    return res.rowCount === 1
  }

  async complete(job: Job, workerId: string): Promise<boolean> {
    const res = await this.pool.query(
      `update ${TABLE}
          set status = 'done', finished_at = now(), locked_until = null, locked_by = null
        where id = $1 and status = 'running' and locked_by = $2 and attempts = $3`,
      [job.id, workerId, job.attempts],
    )
    return res.rowCount === 1
  }

  async fail(job: Job, workerId: string, error: unknown, retryInMs: number): Promise<boolean> {
    const message = error instanceof Error ? (error.stack ?? error.message) : String(error)
    const res = await this.pool.query(
      `update ${TABLE}
          set status       = case when attempts >= max_attempts then 'failed' else 'pending' end,
              finished_at  = case when attempts >= max_attempts then now() end,
              run_at       = case when attempts >= max_attempts then run_at
                                  else now() + ($5::double precision * interval '1 millisecond') end,
              last_error   = $4,
              locked_until = null,
              locked_by    = null
        where id = $1 and status = 'running' and locked_by = $2 and attempts = $3`,
      [job.id, workerId, job.attempts, message, retryInMs],
    )
    return res.rowCount === 1
  }
}

export class Worker<T = unknown> {
  readonly id = randomUUID()
  private readonly concurrency: number
  private readonly pollMs: number
  private readonly lockMs: number
  private readonly backoff: (attempt: number) => number
  private readonly onError: (err: unknown) => void
  private readonly active = new Set<Promise<void>>()
  private stopping = false
  private loop: Promise<void> | null = null
  private wake: () => void = () => {}

  constructor(
    private readonly queue: Queue,
    private readonly name: string,
    private readonly handler: Handler<T>,
    opts: WorkerOptions = {},
  ) {
    this.concurrency = Math.max(1, opts.concurrency ?? 1)
    this.pollMs = opts.pollMs ?? 1000
    this.lockMs = opts.lockMs ?? 30_000
    this.backoff = opts.backoff ?? defaultBackoff
    this.onError = opts.onError ?? ((err) => console.error('pgdurable:', err))
  }

  start(): void {
    if (!this.loop) this.loop = this.run()
  }

  /** Stops taking new jobs and resolves once the ones in flight have finished. */
  async stop(): Promise<void> {
    this.stopping = true
    this.wake()
    await this.loop
  }

  private async run(): Promise<void> {
    while (!this.stopping) {
      const free = this.concurrency - this.active.size
      let claimed: Job<T>[] = []
      if (free > 0) {
        try {
          claimed = await this.queue.claim<T>(this.name, this.id, this.lockMs, free)
        } catch (err) {
          this.onError(err)
        }
      }
      for (const job of claimed) {
        const run: Promise<void> = this.handle(job).finally(() => {
          this.active.delete(run)
          this.wake()
        })
        this.active.add(run)
      }
      // A full batch means there may be more waiting, so ask again at once.
      // Otherwise rest until the poll interval passes or a slot frees up.
      if (free > 0 && claimed.length === free) continue
      await this.sleep(this.pollMs)
    }
    await Promise.all(this.active)
  }

  private async handle(job: Job<T>): Promise<void> {
    const heartbeat = setInterval(() => {
      this.queue.extend(job, this.id, this.lockMs).catch(this.onError)
    }, Math.max(10, Math.floor(this.lockMs / 2)))

    let failure: { error: unknown } | null = null
    try {
      await this.handler(job)
    } catch (error) {
      failure = { error }
    } finally {
      clearInterval(heartbeat)
    }

    // If recording the outcome fails, the claim simply expires and the job is
    // run again. That is the at-least-once guarantee doing its work.
    try {
      if (failure) await this.queue.fail(job, this.id, failure.error, this.backoff(job.attempts))
      else await this.queue.complete(job, this.id)
    } catch (err) {
      this.onError(err)
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const done = () => {
        clearTimeout(timer)
        this.wake = () => {}
        resolve()
      }
      const timer = setTimeout(done, ms)
      this.wake = done
    })
  }
}
