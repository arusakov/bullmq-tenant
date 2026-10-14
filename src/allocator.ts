export interface AllocatorItem<T> {
  /** Unique id of the unit of work; used to release its slot later. */
  key: string
  tenant: string
  /** Lower value = more urgent (same convention as BullMQ). */
  priority: number
  value: T
}

export interface AllocatorOptions {
  /** Max number of items in flight across all tenants. */
  capacity: number
  /** Max number of items in flight for a single tenant. */
  quota?: (tenant: string) => number
}

/**
 * Pure (Redis-free) scheduling core.
 *
 * Holds pending items and decides which one may go next, given the global
 * capacity and per-tenant quotas:
 *  1. the most urgent priority level that has an eligible tenant wins;
 *  2. inside a level the tenant with the fewest items in flight wins
 *     (max-min fairness), ties are broken by "least recently served";
 *  3. tenants that reached their quota are skipped, so the allocator never
 *     idles while there is eligible work (work-conserving).
 */
export class Allocator<T> {
  private readonly pending = new Map<number, Map<string, AllocatorItem<T>[]>>()
  private readonly inFlightKeys = new Map<string, string>()
  private readonly inFlightByTenant = new Map<string, number>()
  private readonly pendingByTenant = new Map<string, number>()
  private readonly lastServed = new Map<string, number>()
  private tick = 0
  private pendingCount = 0

  constructor(private readonly options: AllocatorOptions) {}

  get pendingSize() {
    return this.pendingCount
  }

  get inFlightSize() {
    return this.inFlightKeys.size
  }

  inFlightOf(tenant: string) {
    return this.inFlightByTenant.get(tenant) ?? 0
  }

  /** Pending + in flight items of the tenant. */
  loadOf(tenant: string) {
    return this.inFlightOf(tenant) + (this.pendingByTenant.get(tenant) ?? 0)
  }

  push(item: AllocatorItem<T>) {
    let level = this.pending.get(item.priority)
    if (!level) {
      level = new Map()
      this.pending.set(item.priority, level)
    }
    const queue = level.get(item.tenant)
    if (queue) {
      queue.push(item)
    } else {
      level.set(item.tenant, [item])
    }
    this.pendingCount++
    this.pendingByTenant.set(item.tenant, (this.pendingByTenant.get(item.tenant) ?? 0) + 1)
  }

  /** Takes the next item that may be started now and marks it as in flight. */
  take(): AllocatorItem<T> | undefined {
    if (this.inFlightKeys.size >= this.options.capacity) {
      return undefined
    }

    const priorities = [...this.pending.keys()].sort((a, b) => a - b)
    for (const priority of priorities) {
      const level = this.pending.get(priority)!

      let best: string | undefined
      let bestInFlight = Infinity
      let bestServed = Infinity
      for (const tenant of level.keys()) {
        const inFlight = this.inFlightOf(tenant)
        if (inFlight >= this.quotaOf(tenant)) {
          continue
        }
        const served = this.lastServed.get(tenant) ?? 0
        if (inFlight < bestInFlight || (inFlight === bestInFlight && served < bestServed)) {
          best = tenant
          bestInFlight = inFlight
          bestServed = served
        }
      }

      if (best === undefined) {
        continue
      }

      const queue = level.get(best)!
      const item = queue.shift()!
      if (queue.length === 0) {
        level.delete(best)
        if (level.size === 0) {
          this.pending.delete(priority)
        }
      }
      this.pendingCount--
      this.decPending(best)
      this.markInFlight(item.key, item.tenant)
      this.lastServed.set(best, ++this.tick)
      return item
    }

    return undefined
  }

  /**
   * Marks a unit of work as in flight without going through the queue
   * (e.g. jobs found in the output queues on start). May exceed capacity.
   */
  acquire(key: string, tenant: string) {
    if (this.inFlightKeys.has(key)) {
      return false
    }
    this.markInFlight(key, tenant)
    return true
  }

  /** Frees a slot. Returns false when the key is unknown. */
  release(key: string) {
    const tenant = this.inFlightKeys.get(key)
    if (tenant === undefined) {
      return false
    }
    this.inFlightKeys.delete(key)
    const left = (this.inFlightByTenant.get(tenant) ?? 1) - 1
    if (left > 0) {
      this.inFlightByTenant.set(tenant, left)
    } else {
      this.inFlightByTenant.delete(tenant)
      this.lastServed.delete(tenant)
    }
    return true
  }

  /** Removes and returns everything that is still pending. */
  drain(): AllocatorItem<T>[] {
    const items: AllocatorItem<T>[] = []
    for (const level of this.pending.values()) {
      for (const queue of level.values()) {
        items.push(...queue)
      }
    }
    this.pending.clear()
    this.pendingByTenant.clear()
    this.pendingCount = 0
    return items
  }

  private decPending(tenant: string) {
    const left = (this.pendingByTenant.get(tenant) ?? 1) - 1
    if (left > 0) {
      this.pendingByTenant.set(tenant, left)
    } else {
      this.pendingByTenant.delete(tenant)
    }
  }

  quotaOf(tenant: string) {
    return this.options.quota?.(tenant) ?? Infinity
  }

  private markInFlight(key: string, tenant: string) {
    this.inFlightKeys.set(key, tenant)
    this.inFlightByTenant.set(tenant, this.inFlightOf(tenant) + 1)
  }
}
