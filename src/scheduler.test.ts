import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { afterEach, describe, test } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'

import { Queue, Worker, type Job } from 'bullmq'

import { TenantScheduler } from './scheduler.js'
import type { TenantSchedulerOptions } from './scheduler.js'

const connection = {
  host: process.env['REDIS_HOST'] ?? 'localhost',
  port: Number(process.env['REDIS_PORT'] ?? 6379),
}
// unique prefix per run: tests never touch foreign keys in a shared Redis
const prefix = `bmqt-test-${randomUUID()}`

const closers: Array<() => Promise<unknown>> = []
const queues: Queue[] = []

const queue = (name: string) => {
  const q = new Queue(name, { connection, prefix })
  queues.push(q)
  closers.push(() => q.close())
  return q
}

type Inputs = Array<string | { name: string; priority: number }>

const create = (
  inputs: Inputs,
  routes: Record<string, string>,
  options: TenantSchedulerOptions,
) => {
  const inputsConfig = Object.fromEntries(
    inputs.map((i) => (typeof i === 'string' ? [i, true] : [i.name, { priority: i.priority }])),
  )
  const outputsConfig = Object.fromEntries(Object.values(routes).map((n) => [n, true]))
  const s = new TenantScheduler<string, string, string>(
    inputsConfig,
    outputsConfig,
    { connection, prefix },
    routes,
    options,
  )
  s.on('error', (error) => assert.fail(error))
  return s
}

const scheduler = async (
  inputs: Inputs,
  routes: Record<string, string>,
  options: TenantSchedulerOptions,
) => {
  const s = create(inputs, routes, options)
  await s.waitUntilReady()
  s.run()
  closers.push(() => s.close())
  return s
}

const consumer = (name: string, processor: (job: Job) => Promise<unknown>, concurrency = 1) => {
  const worker = new Worker(name, processor, { connection, prefix, concurrency })
  closers.push(() => worker.close())
  return worker
}

const until = async (predicate: () => boolean | Promise<boolean>, timeout = 5000) => {
  const deadline = Date.now() + timeout
  while (!(await predicate())) {
    if (Date.now() > deadline) {
      assert.fail('timeout')
    }
    await sleep(20)
  }
}

const name = (label: string) => `${label}-${randomUUID().slice(0, 8)}`

afterEach(async () => {
  for (const close of closers.splice(0).reverse()) {
    await close()
  }
  for (const q of queues.splice(0)) {
    const cleaner = new Queue(q.name, { connection, prefix })
    await cleaner.obliterate({ force: true })
    await cleaner.close()
  }
})

describe('TenantScheduler', () => {
  test('routes jobs by name to output queues', async () => {
    const input = name('in')
    const outEmail = name('out-email')
    const outReport = name('out-report')
    const inQ = queue(input)
    const emailQ = queue(outEmail)
    const reportQ = queue(outReport)

    await scheduler([input], { email: outEmail, report: outReport }, { capacity: 10 })

    await inQ.add('email', { tenantId: 't1', to: 'a@b.c' })
    await inQ.add('report', { tenantId: 't2', id: 7 })

    await until(async () => (await emailQ.getWaitingCount()) === 1 && (await reportQ.getWaitingCount()) === 1)

    const [email] = await emailQ.getJobs(['waiting'])
    const [report] = await reportQ.getJobs(['waiting'])
    assert.equal(email?.name, 'email')
    assert.deepEqual(email?.data, { tenantId: 't1', to: 'a@b.c' })
    assert.equal(report?.name, 'report')
    assert.deepEqual(report?.data, { tenantId: 't2', id: 7 })
  })

  test('fails unroutable jobs and jobs without tenant', async () => {
    const input = name('in')
    const out = name('out')
    const inQ = queue(input)
    queue(out)

    await scheduler([input], { known: out }, { capacity: 1 })

    await inQ.add('unknown', { tenantId: 't' })
    await inQ.add('known', {})

    await until(async () => (await inQ.getFailedCount()) === 2)
  })

  test('limits jobs in the output queues by capacity', async () => {
    const input = name('in')
    const out = name('out')
    const inQ = queue(input)
    const outQ = queue(out)

    const s = await scheduler([input], { work: out }, { capacity: 2 })

    await inQ.addBulk(Array.from({ length: 6 }, (_, i) => ({ name: 'work', data: { tenantId: 't', i } })))

    await until(async () => (await inQ.getCompletedCount()) === 2)
    await sleep(200)
    assert.equal(await outQ.getWaitingCount(), 2)
    assert.equal(s.inFlight, 2)

    let done = 0
    consumer(out, async () => void done++)
    await until(() => done === 6)
    assert.equal(await inQ.getCompletedCount(), 6)
  })

  test('applies tenant quotas', async () => {
    const input = name('in')
    const out = name('out')
    const inQ = queue(input)
    const outQ = queue(out)

    const s = await scheduler([input], { work: out }, { capacity: 10, quotas: { tenants: { a: 1 } } })

    await inQ.addBulk([
      ...Array.from({ length: 3 }, () => ({ name: 'work', data: { tenantId: 'a' } })),
      ...Array.from({ length: 3 }, () => ({ name: 'work', data: { tenantId: 'b' } })),
    ])

    await until(async () => (await outQ.getWaitingCount()) === 4)
    await sleep(200)

    const jobs = await outQ.getJobs(['waiting'])
    const tenants = jobs.map((job) => job.data.tenantId as string).sort()
    assert.deepEqual(tenants, ['a', 'b', 'b', 'b'])
    assert.equal(s.inFlightOf('a'), 1)
    assert.equal(s.inFlightOf('b'), 3)

    let done = 0
    consumer(out, async () => void done++)
    await until(() => done === 6)
  })

  test('processes more urgent input queue first', async () => {
    const high = name('high')
    const low = name('low')
    const out = name('out')
    const highQ = queue(high)
    const lowQ = queue(low)
    const outQ = queue(out)

    const s = await scheduler(
      [
        { name: low, priority: 10 },
        { name: high, priority: 1 },
      ],
      { work: out },
      { capacity: 1 },
    )

    // occupy the only slot, then buffer low before high
    await lowQ.add('work', { tenantId: 't', label: 'blocker' })
    await until(async () => (await outQ.getWaitingCount()) === 1)
    await lowQ.add('work', { tenantId: 't', label: 'low' })
    await highQ.add('work', { tenantId: 't', label: 'high' })
    await until(() => s.pending === 2)

    const order: string[] = []
    consumer(out, async (job) => void order.push(job.data.label))
    await until(() => order.length === 3)
    assert.deepEqual(order, ['blocker', 'high', 'low'])
  })

  test('keeps the slot while output job is being retried', async () => {
    const input = name('in')
    const out = name('out')
    const inQ = queue(input)
    queue(out)

    await scheduler([input], { work: out }, { capacity: 1 })

    await inQ.add('work', { tenantId: 't', label: 'x' }, { attempts: 2, backoff: 300 })
    await inQ.add('work', { tenantId: 't', label: 'y' })

    const log: string[] = []
    let failed = false
    consumer(out, async (job) => {
      log.push(job.data.label)
      if (job.data.label === 'x' && !failed) {
        failed = true
        throw new Error('boom')
      }
    })

    await until(() => log.length === 3)
    assert.deepEqual(log, ['x', 'x', 'y'])
  })

  test('close returns buffered jobs to the input queue', async () => {
    const input = name('in')
    const out = name('out')
    const inQ = queue(input)
    queue(out)

    const s = create([input], { work: out }, { capacity: 1 })
    await s.waitUntilReady()
    s.run()

    await inQ.addBulk(Array.from({ length: 3 }, () => ({ name: 'work', data: { tenantId: 't' } })))
    await until(() => s.pending === 2)

    await s.close()

    assert.equal(await inQ.getWaitingCount(), 2)
    assert.equal(await inQ.getCompletedCount(), 1)
    assert.equal(await inQ.getFailedCount(), 0)
  })
})
