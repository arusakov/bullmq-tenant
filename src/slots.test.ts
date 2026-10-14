import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { after, describe, test } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'

import { Redis } from 'ioredis'

import { Slots } from './slots.js'

const client = new Redis({
  host: process.env['REDIS_HOST'] ?? 'localhost',
  port: Number(process.env['REDIS_PORT'] ?? 6379),
})
const bases: string[] = []

const slots = () => {
  const base = `bmqt-slots-test-${randomUUID()}`
  bases.push(base)
  return new Slots(client, base)
}

const reserve = (s: Slots, key: string, tenant = 't', capacity = 10, quota = 10, leaseMs = 10_000) =>
  s.reserve({ tenant, key, capacity, quota, leaseMs })

after(async () => {
  for (const base of bases) {
    const keys = await client.keys(`${base}*`)
    if (keys.length > 0) {
      await client.del(...keys)
    }
  }
  client.disconnect()
})

describe('Slots', () => {
  test('respects global capacity', async () => {
    const s = slots()
    assert.equal(await reserve(s, 'a', 't', 2), 'reserved')
    assert.equal(await reserve(s, 'b', 'u', 2), 'reserved')
    assert.equal(await reserve(s, 'c', 'v', 2), 'full')

    assert.equal(await s.release('a'), true)
    assert.equal(await reserve(s, 'c', 'v', 2), 'reserved')
    assert.equal(await s.size(), 2)
  })

  test('respects tenant quota independently of other tenants', async () => {
    const s = slots()
    assert.equal(await reserve(s, 'a1', 'a', 10, 1), 'reserved')
    assert.equal(await reserve(s, 'a2', 'a', 10, 1), 'quota')
    assert.equal(await reserve(s, 'b1', 'b', 10, 1), 'reserved')
    assert.equal(await s.sizeOf('a'), 1)

    await s.release('a1')
    assert.equal(await reserve(s, 'a2', 'a', 10, 1), 'reserved')
  })

  test('release is idempotent', async () => {
    const s = slots()
    await reserve(s, 'a')
    assert.equal(await s.release('a'), true)
    assert.equal(await s.release('a'), false)
    assert.equal(await s.size(), 0)
    assert.equal(await s.sizeOf('t'), 0)
  })

  test('confirmed slot is reported as held and listed', async () => {
    const s = slots()
    await reserve(s, 'a')
    await reserve(s, 'b')
    await s.confirm('t', 'a')

    assert.equal(await reserve(s, 'a'), 'held')
    assert.deepEqual(await s.confirmed(), ['a'])
  })

  test('unconfirmed slot is returned when its lease ends', async () => {
    const s = slots()
    assert.equal(await reserve(s, 'a', 't', 1, 1, 50), 'reserved')
    assert.equal(await reserve(s, 'b', 't', 1, 1, 50), 'full')

    await sleep(80)
    assert.equal(await reserve(s, 'b', 't', 1, 1, 50), 'reserved')
    assert.equal(await s.size(), 1)
  })

  test('lock is granted to one caller at a time', async () => {
    const s = slots()
    assert.equal(await s.lock('x', 10_000), true)
    assert.equal(await s.lock('x', 10_000), false)
  })
})
