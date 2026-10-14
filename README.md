# bullmq-tenant

Scheduler between BullMQ input and output queues for multi-tenant systems.

```
input queues (by priority) ──► TenantScheduler ──► output queues (by job name)
                               capacity + tenant quotas
```

- jobs are taken from one or several input queues (a lower `priority` value is more urgent);
- the output queue is chosen by job name (`nameToQueue`);
- a job is moved to the output queue only when the system has free `capacity`
  (jobs waiting/active/delayed in output queues) and its tenant has free quota;
- between tenants the slots are shared fairly (the tenant with the fewest jobs in flight goes first).

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
- Fairness is guaranteed inside the `lookahead` window (default 500 jobs per input queue).
- `delay` of an input job is applied by the input queue and is not forwarded.
- On start the scheduler counts jobs that are already in the output queues.
