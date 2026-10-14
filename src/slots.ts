/** The only thing needed from an ioredis client (`Redis` and `Cluster` fit). */
export interface ScriptRunner {
  eval(script: string, numKeys: number, ...args: Array<string | number>): Promise<unknown>
}

export type ReserveResult =
  /** a slot is taken, the caller must add the job and then `confirm` */
  | 'reserved'
  /** the job already has a confirmed slot (it was dispatched before) */
  | 'held'
  /** global capacity is used up */
  | 'full'
  /** the tenant has used up its quota */
  | 'quota'

export interface ReserveOptions {
  tenant: string
  /** Unique id of the unit of work (output job); used to release the slot. */
  key: string
  /** Max number of slots in use across all tenants. */
  capacity: number
  /** Max number of slots in use by the tenant. */
  quota: number
  /** An unconfirmed slot is returned automatically after this time (ms). */
  leaseMs: number
}

// Slots live in Redis, so any number of schedulers share the same limits.
//
//   <base>:slots        ZSET  key -> lease deadline (ms), `inf` once confirmed
//   <base>:tenant:<id>  ZSET  the same, per tenant
//   <base>:owners       HASH  key -> tenant
//
// A slot is reserved before the job is added to the output queue and confirmed
// after it. A scheduler that dies in between leaves an unconfirmed slot that is
// reaped when its lease ends. Confirmed slots are released by queue events
// (idempotent: only the first release counts) and by `confirmed()`-based
// reconciliation, if an event was missed.

const RESERVE = `
local slots, owners = KEYS[1], KEYS[2]
local base, now, capacity, quota = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
local tenant, key, lease = ARGV[5], ARGV[6], tonumber(ARGV[7])

local expired = redis.call('ZRANGEBYSCORE', slots, '-inf', now, 'LIMIT', 0, 100)
for _, k in ipairs(expired) do
  local t = redis.call('HGET', owners, k)
  redis.call('ZREM', slots, k)
  if t then
    redis.call('ZREM', base .. t, k)
    redis.call('HDEL', owners, k)
  end
end

local tkey = base .. tenant
local score = redis.call('ZSCORE', slots, key)
if score then
  if score == 'inf' or score == '+inf' then
    return 2
  end
  redis.call('ZADD', slots, now + lease, key)
  redis.call('ZADD', tkey, now + lease, key)
  return 1
end
if redis.call('ZCARD', slots) >= capacity then
  return 0
end
if redis.call('ZCARD', tkey) >= quota then
  return -1
end
redis.call('ZADD', slots, now + lease, key)
redis.call('ZADD', tkey, now + lease, key)
redis.call('HSET', owners, key, tenant)
return 1
`

const CONFIRM = `
redis.call('ZADD', KEYS[1], 'inf', ARGV[2])
redis.call('ZADD', ARGV[1] .. ARGV[3], 'inf', ARGV[2])
redis.call('HSET', KEYS[2], ARGV[2], ARGV[3])
return 1
`

const RELEASE = `
local t = redis.call('HGET', KEYS[2], ARGV[2])
if not t then
  return 0
end
redis.call('ZREM', KEYS[1], ARGV[2])
redis.call('ZREM', ARGV[1] .. t, ARGV[2])
redis.call('HDEL', KEYS[2], ARGV[2])
return 1
`

const LOCK = `
if redis.call('SET', KEYS[1], '1', 'NX', 'PX', ARGV[1]) then
  return 1
end
return 0
`

const RESERVE_RESULT: Record<number, ReserveResult> = {
  1: 'reserved',
  2: 'held',
  0: 'full',
  [-1]: 'quota',
}

export class Slots {
  private readonly slots: string
  private readonly owners: string
  private readonly tenantBase: string

  /** @param base key prefix; use a hash tag (`{...}`) to be Redis Cluster safe. */
  constructor(
    private readonly client: ScriptRunner,
    private readonly base: string,
  ) {
    this.slots = `${base}:slots`
    this.owners = `${base}:owners`
    this.tenantBase = `${base}:tenant:`
  }

  async reserve({ tenant, key, capacity, quota, leaseMs }: ReserveOptions) {
    const result = await this.client.eval(
      RESERVE, 2, this.slots, this.owners,
      this.tenantBase, Date.now(), capacity, quota, tenant, key, leaseMs,
    )
    return RESERVE_RESULT[Number(result)] ?? 'full'
  }

  async confirm(tenant: string, key: string) {
    await this.client.eval(CONFIRM, 2, this.slots, this.owners, this.tenantBase, key, tenant)
  }

  /** Frees a slot. Returns false when it is not held (already released). */
  async release(key: string) {
    const result = await this.client.eval(RELEASE, 2, this.slots, this.owners, this.tenantBase, key)
    return Number(result) === 1
  }

  async size() {
    return Number(await this.client.eval(`return redis.call('ZCARD', KEYS[1])`, 1, this.slots))
  }

  async sizeOf(tenant: string) {
    return Number(
      await this.client.eval(`return redis.call('ZCARD', KEYS[1])`, 1, this.tenantBase + tenant),
    )
  }

  /** Keys of the slots whose job was added to an output queue. */
  async confirmed() {
    const keys = await this.client.eval(
      `return redis.call('ZRANGEBYSCORE', KEYS[1], 'inf', 'inf')`,
      1,
      this.slots,
    )
    return keys as string[]
  }

  /** Cluster-wide mutex for periodic jobs: true for one caller per `ttlMs`. */
  async lock(name: string, ttlMs: number) {
    return Number(await this.client.eval(LOCK, 1, `${this.base}:lock:${name}`, ttlMs)) === 1
  }
}
