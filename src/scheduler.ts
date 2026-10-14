import { EventEmitter } from 'node:events'

import {
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

import { Allocator } from './allocator.js'
import type { AllocatorItem } from './allocator.js'

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
   */
  capacity: number
  quotas?: {
    /** Max in-flight jobs per tenant. Default: `capacity` (no extra limit). */
    default?: number
    /** Per-tenant overrides. */
    tenants?: Record<string, number>
  }
  /** Extracts tenant id from a job. Default: `job.data.tenantId`. */
  tenantOf?: (job: Job<any, any, JN>) => string | number | undefined | null
  /**
   * How many jobs per input queue the scheduler may hold in memory to choose
   * from. Fairness is only guaranteed inside this window. Default: 500.
   */
  lookahead?: number
  /** Last chance to change a job before it is added to the output queue. */
  prepareJob?: (job: DefaultJob<JN>) => DefaultJob<JN>
}

type Pending = {
  job: Job
  token: string | undefined
  outputName: string
  resolve: () => void
  reject: (error: Error) => void
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
 * Jobs are taken from the input queues, buffered, and moved to the output
 * queue chosen by job name only when the system has free capacity and the
 * job's tenant has free quota. Output queues are consumed by ordinary BullMQ
 * workers (e.g. `WorkerManager` from bullmq-toolkit).
 *
 * Emits 'error' (like BullMQ workers do); attach a listener.
 */
export class TenantScheduler<
  JNs extends string,
  INs extends string,
  ONs extends string,
> extends EventEmitter {
  protected readonly workers = {} as Record<INs, Worker>
  protected readonly queues = {} as Record<ONs, Queue>
  protected readonly events = {} as Record<ONs, QueueEvents>
  protected readonly priorities = {} as Record<INs, number>
  protected readonly allocator: Allocator<Pending>
  protected readonly prepareJob: NonNullable<TenantSchedulerOptions<JNs>['prepareJob']>
  protected readonly tenantOf: NonNullable<TenantSchedulerOptions<JNs>['tenantOf']>
  protected connected = false
  protected closing = false

  constructor(
    inputsConfig: Inputs<INs>,
    outputsConfig: Outputs<ONs>,
    queueOptions: SchedulerQueueOptions,
    protected nameToQueue: NameToQueue<JNs, ONs>,
    protected options: TenantSchedulerOptions<JNs>,
  ) {
    super()

    if (!(options.capacity > 0)) {
      throw new RangeError('capacity must be a positive number')
    }

    const { quotas } = options
    this.allocator = new Allocator<Pending>({
      capacity: options.capacity,
      quota: (tenant) => quotas?.tenants?.[tenant] ?? quotas?.default ?? options.capacity,
    })
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
        if (this.allocator.release(`${name}:${jobId}`)) {
          this.pump()
        }
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
          concurrency: options.lookahead ?? 500,
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
  }

  /** Number of jobs buffered and waiting for a free slot. */
  get pending() {
    return this.allocator.pendingSize
  }

  /** Number of jobs dispatched to the output queues and not finished yet. */
  get inFlight() {
    return this.allocator.inFlightSize
  }

  inFlightOf(tenant: string) {
    return this.allocator.inFlightOf(tenant)
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

    // account for jobs that are already in the output queues (restart)
    for (const [name, queue] of Object.entries<Queue>(this.queues)) {
      const jobs = await queue.getJobs(['waiting', 'active', 'delayed', 'prioritized'])
      for (const job of jobs) {
        const tenant = this.tenantKey(job)
        if (tenant !== undefined && job.id !== undefined) {
          this.allocator.acquire(`${name}:${job.id}`, tenant)
        }
      }
    }

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
  }

  /**
   * Stops the scheduler. Buffered jobs are returned to their input queues,
   * jobs already dispatched stay in the output queues.
   */
  async close() {
    if (this.closing) {
      return false
    }
    this.closing = true

    // stop fetching first, otherwise returned jobs are picked up again at once
    await Promise.all(this.getInputWorkers().map((worker) => worker.pause(true)))

    const buffered = this.allocator.drain()
    await Promise.all(
      buffered.map(async ({ value }) => {
        try {
          await value.job.moveToWait(value.token)
          value.reject(new WaitingError())
        } catch (error) {
          value.reject(error as Error)
        }
      }),
    )

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

  protected accept(inputName: INs, job: Job, token: string | undefined) {
    const tenant = this.tenantKey(job)
    if (tenant === undefined) {
      throw new UnrecoverableError(`Job ${job.id} has no tenant id`)
    }

    const outputName = this.getQueueNameByJobName(job.name as JNs)
    if (outputName === undefined || !this.queues[outputName]) {
      throw new UnrecoverableError(`No output queue for job name "${job.name}"`)
    }

    if (this.closing) {
      return job.moveToWait(token).then(() => {
        throw new WaitingError()
      })
    }

    return new Promise<void>((resolve, reject) => {
      this.allocator.push({
        key: `${outputName}:${this.outputJobId(inputName, job)}`,
        tenant,
        priority: this.priorities[inputName],
        value: { job, token, outputName, resolve, reject },
      })
      this.pump()
    })
  }

  protected pump() {
    if (this.closing) {
      return
    }
    for (let item = this.allocator.take(); item; item = this.allocator.take()) {
      void this.dispatch(item)
    }
  }

  protected async dispatch(item: AllocatorItem<Pending>) {
    const { job, outputName, resolve, reject } = item.value
    const queue = this.queues[outputName as ONs]
    const jobId = item.key.slice(outputName.length + 1)

    try {
      // the same input job can be delivered twice (stalled, crash after add)
      const existing = await queue.getJob(jobId)
      if (existing) {
        this.allocator.release(item.key)
      } else {
        const opts: JobsOptions = { jobId }
        for (const name of FORWARDED_OPTIONS) {
          if (job.opts[name] !== undefined) {
            Object.assign(opts, { [name]: job.opts[name] })
          }
        }
        const prepared = this.prepareJob({ name: job.name as JNs, data: job.data, opts })
        await queue.add(prepared.name, prepared.data, prepared.opts)
      }
      resolve()
    } catch (error) {
      this.allocator.release(item.key)
      reject(error as Error)
    }
    this.pump()
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
