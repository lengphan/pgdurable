// npm run example
import pg from 'pg'
import { Queue } from '../src/index.js'

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL })
const queue = new Queue(pool)
await queue.migrate()

// The key makes this safe to call twice: a retried HTTP request, a double
// click, a webhook delivered again.
await queue.enqueue('receipts', { orderId: 1001 }, { idempotencyKey: 'receipt-1001' })
const again = await queue.enqueue('receipts', { orderId: 1001 }, { idempotencyKey: 'receipt-1001' })
console.log('second enqueue deduplicated:', again.deduplicated)

await queue.enqueue('receipts', { orderId: 1002 }, { delayMs: 1500 })

let flaky = true
const worker = queue.work<{ orderId: number }>(
  'receipts',
  async (job) => {
    if (job.payload.orderId === 1001 && flaky) {
      flaky = false
      throw new Error('mail server timed out')
    }
    console.log(`sent receipt for order ${job.payload.orderId} (attempt ${job.attempts})`)
  },
  { pollMs: 100, backoff: () => 500 },
)

await new Promise((r) => setTimeout(r, 3000))
await worker.stop()
console.log(await queue.stats('receipts'))
await pool.end()
