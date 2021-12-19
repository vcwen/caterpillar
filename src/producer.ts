import Redis, { RedisOptions } from 'ioredis'
import { IPayload } from './message'

export class Producer {
  private redis: Redis.Redis
  private stream: string
  constructor(stream: string, connection?: RedisOptions) {
    this.stream = stream
    this.redis = new Redis(connection)
  }
  public async produce(
    data: Record<string, unknown>,
    header: Record<string, unknown> = {}
  ): Promise<string> {
    const payload: IPayload = { header, body: data }
    const val = JSON.stringify(payload)
    return this.redis.xadd(this.stream, '*', 'payload', val)
  }
}

export const producer = new Producer('cater', {
  host: '192.168.1.200'
})

producer
  .produce(
    {
      type: 'text',
      time: new Date().toISOString(),
      text: 'hello vc'
    },
    {}
  )
  // eslint-disable-next-line no-console
  .then((id) => console.log('new message produced:', id))
  // eslint-disable-next-line no-console
  .catch((err) => console.error(err))
  .finally(() => process.exit())
