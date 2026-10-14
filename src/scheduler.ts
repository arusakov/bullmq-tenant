import { EventEmitter } from 'node:events'

import {
  DelayedError,
  Queue,
  QueueEvents,
  UnrecoverableError,
  WaitingError,
  Worker,
} from 'bullmq'
import type {
  BaseJobOptions,
  Job,
  JobsOptions,
  QueueOptions,
  WorkerOptions,
} from 'bullmq'

import { Slots } from './slots.js'
import type { ScriptRunner } from './slots.js'

/** Connection (and key prefix) shared by all input and output queues. */
export type SchedulerQueueOptions = Pick<QueueOptions, 'connection' | 'prefix'>

export type DefaultJob<JN extends string> = {
  name: JN
  data: unknown
  opts?: BaseJobOptions
}
export type NameToQueue<JN extends string, QN extends string> = Record<JN, QN>

/**
 * Input queue config. `true` = defaults, falsy = queue is not consumed.
 * `priority`: lower value = more urgent, default 0.
 */
export type InputConfig = (Partial<WorkerOptions> & { priority?: number }) | boolean | undefined | null
export type Inputs<IN extends string> = Record<IN, InputConfig>

/** Output queue config. `true` = defaults, falsy = queue is not used. */
export type OutputConfig = Partial<QueueOptions> | boolean | undefined | null
export type Outputs<ON extends string> = Record<ON, OutputConfig>

export type TenantSchedulerOptions<JN extends string = string> = {
  /**
   * Max number of jobs in the output queues (waiting + active + delayed)
   * at the same time, i.e. the throughput of the whole system.
   * Shared by all schedulers with the same `name`, `prefix` and Redis.
   */
  capacity: number
  quotas?: {
    /** Max in-flight jobs per tenant. Default: `capacity` (no extra limit). */
    default?: number
    /** Per-tenant overrides. */
    tenants?: Record<string, number>
  }
  /**
   * Schedulers with the same name (and prefix, Redis) run in parallel and
   * share capacity and quotas. Default: 'default'.
   */
  name?: string
  /** Extracts tenant id from a job. Default: `job.data.tenantId`. */
  tenantOf?: (job: Job<any, any, JN>) => string | number | undefined | null
  /** Last chance to change a job before it is added to the output queue. */
  prepareJob?: (job: DefaultJob<JN>) => DefaultJob<JN>
  /**
   * A job that cannot get a slot goes back to its input queue as delayed.
   * Base delay (ms) when the tenant has used up its quota. Default: 1000.
   */
  quotaDelay?: number
  /** Base delay (ms) when the global capacity is used up. Default: 500. */
  capacityDelay?: number
  /**
   * Delays grow by 1.5x with every attempt up to this limit (ms) and are
   * multiplied by (1 + rank of the input queue priority), so less urgent
   * queues retry less often. Default: 10000.
   */
  maxDelay?: number
  /** How long (ms) a reserved slot is kept if the scheduler dies before adding the job. Default: 30000. */
  leaseTime?: number
  /** How often (ms) slots of finished/removed jobs whose event was missed are released. Default: 30000. */
  reconcileInterval?: number
}

export type DispatchedEvent = {
  tenant: string
  inputQueue: string
  outputQueue: string
  /** Id of the job in the input queue. */
  jobId: string | undefined
  /** Id of the job in the output queue. */
  outputJobId: string
}

export type DelayedEvent = Omit<DispatchedEvent, 'outputJobId'> & {
  /** `quota`: the tenant has no free quota, `capacity`: the system has no free capacity. */
  reason: 'quota' | 'capacity'
  /** ms before the job is checked again. */
  delay: number
}

export type ReleasedEvent = {
  outputQueue: string
  /** Id of the job in the output queue. */
  outputJobId: string
}

export type SchedulerEvents = {
  /** A job was moved to an output queue. */
  dispatched: [DispatchedEvent]
  /** A job got no slot and went back to its input queue as delayed. */
  delayed: [DelayedEvent]
  /** A job in an output queue finished (or was removed) and its slot was freed. */
  released: [ReleasedEvent]
  error: [Error]
}

const FORWARDED_OPTIONS = [
  'attempts',
  'backoff',
  'priority',
  'lifo',
  'removeOnComplete',
  'removeOnFail',
  'keepLogs',
  'stackTraceLimit',
  'sizeLimit',
] as const satisfies ReadonlyArray<keyof JobsOptions>

const PREPARE_JOB_DEFAULT = <J>(job: J) => job

/**
 * Sits between input queues and output queues.
 *
 * A job is taken from an input queue and moved to the output queue chosen by
 * job name only when the system has free capacity and the job's tenant has
 * free quota. Otherwise it goes back to the input queue as delayed and is
 * retried later, so a tenant with a huge backlog never blocks the others.
 *
 * Slots are kept in Redis, so any number of schedulers (processes) with the
 * same `name` can run in parallel and share the limits. Output queues are
 * consumed by ordinary BullMQ workers (e.g. `WorkerManager` from bullmq-toolkit).
 *
 * Emits 'dispatched', 'delayed' and 'released' (see `SchedulerEvents`), e.g. for
 * logging and metrics. Emits 'error' (like BullMQ workers do); attach a listener.
 */
export class TenantScheduler<
  JNs extends string,
  INs extends string,
  ONs extends string,
> extends EventEmitter<SchedulerEvents> {
  protected readonly workers = {} as Record<INs, Worker>
  protected readonly queues = {} as Record<ONs, Queue>
  protected readonly events = {} as Record<ONs, QueueEvents>
  protected readonly priorities = {} as Record<INs, number>
  /** Index of the input queue priority among all priorities: 0 = most urgent. */
  protected readonly ranks = {} as Record<INs, number>
  protected readonly prepareJob: NonNullable<TenantSchedulerOptions<JNs>['prepareJob']>
  protected readonly tenantOf: NonNullable<TenantSchedulerOptions<JNs>['tenantOf']>
  protected slots!: Slots
  protected reconciler: NodeJS.Timeout | undefined
  protected connected = false
  protected closing = false

  constructor(
    inputsConfig: Inputs<INs>,
    outputsConfig: Outputs<ONs>,
    protected queueOptions: SchedulerQueueOptions,
    protected nameToQueue: NameToQueue<JNs, ONs>,
    protected options: TenantSchedulerOptions<JNs>,
  ) {
    super()

    if (!(options.capacity > 0)) {
      throw new RangeError('capacity must be a positive number')
    }

    this.prepareJob = options.prepareJob ?? PREPARE_JOB_DEFAULT
    this.tenantOf = options.tenantOf ?? ((job) => job.data?.tenantId)

    for (const [name, config] of Object.entries<OutputConfig>(outputsConfig)) {
      if (!config) {
        continue
      }
      const opts = { ...queueOptions, ...(config === true ? undefined : config) }
      this.queues[name as ONs] = new Queue(name, opts)

      const events = new QueueEvents(name, {
        connection: opts.connection,
        ...(opts.prefix === undefined ? undefined : { prefix: opts.prefix }),
      })
      events.on('error', (error) => this.emit('error', error))
      const release = ({ jobId }: { jobId: string }) => {
        this.slots
          ?.release(`${name}:${jobId}`)
          .then((released) => {
            if (released) {
              this.emit('released', { outputQueue: name, outputJobId: jobId })
            }
          })
          .catch((error) => this.emit('error', error))
      }
      // 'failed' is emitted only when attempts are exhausted, retries keep the slot
      events.on('completed', release)
      events.on('failed', release)
      events.on('removed', release)
      this.events[name as ONs] = events
    }

    if (Object.keys(this.queues).length === 0) {
      throw new RangeError('at least one output queue is required')
    }

    for (const [name, config] of Object.entries<InputConfig>(inputsConfig)) {
      if (!config) {
        continue
      }
      const { priority = 0, ...workerOptions } = config === true ? {} : config
      this.priorities[name as INs] = priority

      const worker = new Worker(
        name,
        (job, token) => this.accept(name as INs, job, token),
        {
          ...queueOptions,
          concurrency: 100,
          ...workerOptions,
          autorun: false,
        },
      )
      worker.on('error', (error) => this.emit('error', error))
      this.workers[name as INs] = worker
    }

    if (Object.keys(this.workers).length === 0) {
      throw new RangeError('at least one input queue is required')
    }

    const levels = [...new Set(Object.values<number>(this.priorities))].sort((a, b) => a - b)
    for (const [name, priority] of Object.entries<number>(this.priorities)) {
      this.ranks[name as INs] = levels.indexOf(priority)
    }
  }

  /** Number of jobs dispatched to the output queues and not finished yet (all schedulers). */
  async inFlight() {
    this.checkConnected()
    return this.slots.size()
  }

  async inFlightOf(tenant: string) {
    this.checkConnected()
    return this.slots.sizeOf(tenant)
  }

  async waitUntilReady() {
    if (this.connected) {
      return false
    }

    await Promise.all([
      ...this.getOutputQueues().map((queue) => queue.waitUntilReady()),
      ...this.getEvents().map((events) => events.waitUntilReady()),
      ...this.getInputWorkers().map((worker) => worker.waitUntilReady()),
    ])

    const client = (await this.getOutputQueues()[0]!.getBackend().client) as unknown as ScriptRunner
    const prefix = this.queueOptions.prefix ?? 'bull'
    this.slots = new Slots(client, `${prefix}:{tenant-scheduler:${this.options.name ?? 'default'}}`)

    this.connected = true
    return true
  }

  /** Starts consuming the input queues. Call after `waitUntilReady()`. */
  run() {
    this.checkConnected()

    for (const worker of this.getInputWorkers()) {
      if (!worker.isRunning()) {
        void worker.run()
      }
    }

    const interval = this.options.reconcileInterval ?? 30_000
    this.reconciler ??= setInterval(() => {
      this.reconcile(interval).catch((error) => this.emit('error', error))
    }, interval)
    this.reconciler.unref()
  }

  /**
   * Stops the scheduler. Jobs already dispatched stay in the output queues,
   * their slots stay reserved until they finish.
   */
  async close() {
    if (this.closing) {
      return false
    }
    this.closing = true
    clearInterval(this.reconciler)

    await Promise.all(this.getInputWorkers().map((worker) => worker.close()))
    await Promise.all(this.getEvents().map((events) => events.close()))
    await Promise.all(this.getOutputQueues().map((queue) => queue.close()))
    this.connected = false
    return true
  }

  getInputWorker(name: INs) {
    return this.workers[name]
  }

  getInputWorkers() {
    return Object.values<Worker>(this.workers)
  }

  getOutputQueue(name: ONs) {
    return this.queues[name]
  }

  getOutputQueues() {
    return Object.values<Queue>(this.queues)
  }

  getQueueNameByJobName(name: JNs) {
    return this.nameToQueue[name]
  }

  protected getEvents() {
    return Object.values<QueueEvents>(this.events)
  }

  protected async accept(inputName: INs, job: Job, token: string | undefined) {
    const tenant = this.tenantKey(job)
    if (tenant === undefined) {
      throw new UnrecoverableError(`Job ${job.id} has no tenant id`)
    }

    const outputName = this.getQueueNameByJobName(job.name as JNs)
    const queue = outputName === undefined ? undefined : this.queues[outputName]
    if (outputName === undefined || !queue) {
      throw new UnrecoverableError(`No output queue for job name "${job.name}"`)
    }

    const jobId = this.outputJobId(inputName, job)
    const key = `${outputName}:${jobId}`

    const { quotas } = this.options
    const reserved = await this.slots.reserve({
      tenant,
      key,
      capacity: this.options.capacity,
      quota: quotas?.tenants?.[tenant] ?? quotas?.default ?? this.options.capacity,
      leaseMs: this.options.leaseTime ?? 30_000,
    })

    if (reserved === 'held') {
      return // already dispatched (input job was delivered twice)
    }
    if (reserved !== 'reserved') {
      const base = reserved === 'quota' ? this.options.quotaDelay ?? 1000 : this.options.capacityDelay ?? 500
      const delay = this.delayFor(base, inputName, job)
      await job.moveToDelayed(Date.now() + delay, token)
      this.emit('delayed', { tenant, inputQueue: inputName, outputQueue: outputName, jobId: job.id, reason: reserved === 'quota' ? 'quota' : 'capacity', delay })
      throw new DelayedError()
    }

    try {
      // the same input job can be delivered twice (stalled, crash after add)
      if (await queue.getJob(jobId)) {
        await this.slots.release(key)
        return
      }

      const opts: JobsOptions = { jobId }
      for (const name of FORWARDED_OPTIONS) {
        if (job.opts[name] !== undefined) {
          Object.assign(opts, { [name]: job.opts[name] })
        }
      }
      const prepared = this.prepareJob({ name: job.name as JNs, data: job.data, opts })
      await queue.add(prepared.name, prepared.data, prepared.opts)
      await this.slots.confirm(tenant, key)
      this.emit('dispatched', { tenant, inputQueue: inputName, outputQueue: outputName, jobId: job.id, outputJobId: jobId })
    } catch (error) {
      await this.slots.release(key).catch(() => undefined)
      throw error
    }
  }

  protected delayFor(base: number, inputName: INs, job: Job) {
    const attempt = Math.min(job.attemptsStarted || 1, 20)
    const delay = Math.min(
      base * (1 + this.ranks[inputName]) * 1.5 ** (attempt - 1),
      this.options.maxDelay ?? 10_000,
    )
    return Math.round(delay * (0.8 + Math.random() * 0.4))
  }

  /** Releases slots of jobs that finished or were removed while no scheduler listened. */
  protected async reconcile(interval: number) {
    if (this.closing || !(await this.slots.lock('reconcile', interval))) {
      return
    }
    for (const key of await this.slots.confirmed()) {
      const separator = key.indexOf(':')
      const queue = this.queues[key.slice(0, separator) as ONs]
      if (!queue) {
        continue // dispatched by a scheduler with other output queues
      }
      const state = await queue.getJobState(key.slice(separator + 1))
      if (state === 'completed' || state === 'failed' || state === 'unknown') {
        await this.slots.release(key)
      }
    }
  }

  /** Input queues have independent id sequences, so the id must be qualified. */
  protected outputJobId(inputName: string, job: Job) {
    return `${inputName}-${job.id}`
  }

  protected tenantKey(job: Job) {
    try {
      const tenant = this.tenantOf(job as Job<any, any, JNs>)
      return tenant === undefined || tenant === null || tenant === '' ? undefined : String(tenant)
    } catch {
      return undefined
    }
  }

  protected checkConnected() {
    if (!this.connected) {
      throw new Error(`${this.constructor.name} is not connected`)
    }
  }
}
