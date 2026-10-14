import { Redis } from 'ioredis'

/** Tests do not need durability: no snapshots, no AOF, nothing is written to disk. */
export const disablePersistence = async (options: { host: string; port: number }) => {
  const client = new Redis(options)
  try {
    await client.config('SET', 'save', '')
    await client.config('SET', 'appendonly', 'no')
  } catch {
    // managed Redis may forbid CONFIG; persistence is then the server's business
  } finally {
    client.disconnect()
  }
}
