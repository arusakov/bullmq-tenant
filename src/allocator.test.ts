import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import { Allocator } from './allocator.js'

const item = (key: string, tenant: string, priority = 0) => ({ key, tenant, priority, value: key })

const drain = (allocator: Allocator<string>) => {
  const keys: string[] = []
  for (let next = allocator.take(); next; next = allocator.take()) {
    keys.push(next.key)
  }
  return keys
}

describe('Allocator', () => {
  test('respects global capacity', () => {
    const allocator = new Allocator<string>({ capacity: 2 })
    for (let i = 0; i < 5; i++) {
      allocator.push(item(`j${i}`, 't'))
    }

    assert.deepEqual(drain(allocator), ['j0', 'j1'])
    assert.equal(allocator.pendingSize, 3)

    allocator.release('j0')
    assert.deepEqual(drain(allocator), ['j2'])
  })

  test('respects tenant quota and does not idle', () => {
    const allocator = new Allocator<string>({
      capacity: 10,
      quota: (tenant) => (tenant === 'a' ? 1 : 10),
    })
    allocator.push(item('a1', 'a'))
    allocator.push(item('a2', 'a'))
    allocator.push(item('b1', 'b'))
    allocator.push(item('b2', 'b'))

    assert.deepEqual(drain(allocator), ['a1', 'b1', 'b2'])

    allocator.release('a1')
    assert.deepEqual(drain(allocator), ['a2'])
  })

  test('is fair between tenants', () => {
    const allocator = new Allocator<string>({ capacity: 4 })
    for (let i = 0; i < 10; i++) {
      allocator.push(item(`a${i}`, 'a'))
    }
    allocator.push(item('b0', 'b'))
    allocator.push(item('c0', 'c'))

    // a was queued first, but b and c must not wait behind all of a's jobs
    const taken = drain(allocator)
    assert.equal(taken.length, 4)
    assert.ok(taken.includes('b0'))
    assert.ok(taken.includes('c0'))
  })

  test('more urgent priority goes first, lower one fills the gaps', () => {
    const allocator = new Allocator<string>({
      capacity: 10,
      quota: () => 1,
    })
    allocator.push(item('low-a', 'a', 5))
    allocator.push(item('low-b', 'b', 5))
    allocator.push(item('high-a', 'a', 1))

    // a is busy with its high priority job, so b's low priority job runs
    assert.deepEqual(drain(allocator), ['high-a', 'low-b'])
  })

  test('acquire counts toward capacity and release is idempotent', () => {
    const allocator = new Allocator<string>({ capacity: 1 })
    assert.equal(allocator.acquire('x', 'a'), true)
    assert.equal(allocator.acquire('x', 'a'), false)
    allocator.push(item('j', 'a'))
    assert.equal(allocator.take(), undefined)

    assert.equal(allocator.release('x'), true)
    assert.equal(allocator.release('x'), false)
    assert.equal(allocator.take()?.key, 'j')
  })

  test('drain returns pending items', () => {
    const allocator = new Allocator<string>({ capacity: 1 })
    allocator.push(item('a', 't'))
    allocator.push(item('b', 't'))
    allocator.take()

    assert.deepEqual(allocator.drain().map((i) => i.key), ['b'])
    assert.equal(allocator.pendingSize, 0)
  })
})
