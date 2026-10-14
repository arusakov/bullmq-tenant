# bullmq-tenant

Scheduler between BullMQ input and output queues for multi-tenant systems.

```
input queues (by priority) ──► TenantScheduler ──► output queues (by job name)
                               capacity + tenant quotas
```

- jobs are taken from one or several input queues (a lower `priority` value is more urgent);
- the output queue is chosen by job name (`nameToQueue`);
- a job is moved to the output queue only when the system has a free `capacity` slot
  (jobs waiting/active/delayed in output queues) and its tenant has free quota;
- otherwise the job goes back to its input queue as delayed (`quotaDelay` / `capacityDelay`,
  growing up to `maxDelay`, with jitter) and is retried later. A tenant with thousands of jobs
  therefore holds at most its quota of slots and never hides the other tenants.

Slots are kept in Redis (Lua scripts), so **any number of schedulers can run in parallel**
(other processes/hosts) and share `capacity` and quotas, as long as they use the same
`name` (default `'default'`), `prefix` and Redis.

```ts
import { TenantScheduler } from 'bullmq-tenant'

const scheduler = new TenantScheduler(
  { 'in-high': { priority: 1 }, 'in-low': { priority: 10 } }, // input queues
  { email: true, report: true },                              // output queues
  { connection },
  { sendEmail: 'email', buildReport: 'report' },              // job name -> output queue
  {
    capacity: 100,
    quotas: { default: 10, tenants: { vip: 50 } },
    // tenantOf: (job) => job.data.tenantId, // default
  },
)

scheduler.on('error', console.error)
await scheduler.waitUntilReady()
scheduler.run()
```

Output queues are consumed by ordinary BullMQ workers. Job data must contain the
tenant id (`data.tenantId` by default); jobs without tenant or route fail permanently.

## Notes

- Delivery is at-least-once. The output job id is `<inputQueue>-<inputJobId>`, so a re-delivered input job is not duplicated.
- A slot is reserved, the job is added to the output queue, the slot is confirmed. If a scheduler dies in between, the reservation expires after `leaseTime` (30s).
- A slot is freed by the `completed`/`failed`/`removed` events of the output queue; a periodic reconcile (`reconcileInterval`, 30s, one scheduler at a time) frees the slots of jobs whose event was missed.
- Priorities and fairness between tenants when the global `capacity` is exhausted are approximate: there is no shared pending list, a free slot goes to the job that retries first. Less urgent input queues retry less often (delay is multiplied by `1 + rank of priority`). The tenant quota is strict.
- Retries cost Redis round trips: a large backlog of one tenant is polled every `quotaDelay`..`maxDelay`; tune the delays to your load.
- Input workers only reserve slots, their `concurrency` (default 100) limits parallel reservations.
- `delay` of an input job is applied by the input queue and is not forwarded.
- Jobs that are already in the output queues when the first scheduler starts are not counted (no adoption); run with an empty namespace or let them finish first.
