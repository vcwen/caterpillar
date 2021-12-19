import Redis, { RedisOptions } from 'ioredis'
import _ from 'lodash'
import { v4 as uuid } from 'uuid'
import { Logger } from 'winston'
import { createLogger } from './logger'
import {
  IPayload,
  Message,
  MessageEventEmitter,
  PendingMessageMetadata
} from './message'
import { TaskPool } from './task-pool'

export class Consumer extends MessageEventEmitter {
  private mainConnection: Redis.Redis
  private secondaryConnection: Redis.Redis
  private taskPool: TaskPool
  private fetchSize = 1
  private timeout = 30000
  private logger: Logger
  constructor(
    private id: string,
    private stream: string,
    private group: string,
    private onMessage: (message: Message) => Promise<void>,
    options?: { connection?: RedisOptions; timeout?: number }
  ) {
    super()
    this.mainConnection = new Redis(options?.connection)
    this.secondaryConnection = new Redis(options?.connection)
    this.taskPool = new TaskPool()
    this.logger = createLogger(`Consumer:${stream}:${group}:${id}`)
  }
  public async start(): Promise<void> {
    await this.createGroup()
    await this.consumePendingMessages().then(() => {
      this.logger.info('start consuming messages')
      this.consumeNewMessages()
      this.consumeStaleMessages()
    })
  }

  private async createGroup(): Promise<void> {
    try {
      await this.mainConnection.xgroup(
        'CREATE',
        this.stream,
        this.group,
        '0',
        'MKSTREAM'
      )
    } catch (err) {
      // ignore error: 'BUSYGROUP Consumer Group name already exists'
    }
  }
  private async consumeNewMessages() {
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const streams = await this.mainConnection.xreadgroup(
        'GROUP',
        this.group,
        this.id,
        'COUNT',
        this.fetchSize,
        'BLOCK',
        1000,
        'STREAMS',
        this.stream,
        '>'
      )
      if (_.isEmpty(streams)) {
        return
      }
      const [stream] = streams
      const [, messages] = stream
      await this.consumeMessages(messages)
    }
  }
  private async consumePendingMessages() {
    let lastMessageId = '0'
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const streams = await this.mainConnection.xreadgroup(
        'GROUP',
        this.group,
        this.id,
        'COUNT',
        this.fetchSize,
        'STREAMS',
        this.stream,
        lastMessageId
      )
      if (_.isEmpty(streams)) {
        return
      }
      const [stream] = streams
      const [, messages] = stream
      if (_.isEmpty(messages)) {
        return
      } else {
        lastMessageId = messages[messages.length - 1][0]
        await this.consumeMessages(messages)
      }
    }
  }
  private async consumeStaleMessages() {
    let lastId = '-'
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const pendingMessages: any[] = await this.secondaryConnection.xpending(
        this.stream,
        this.group,
        lastId,
        '+',
        this.fetchSize
      )
      if (_.isEmpty(pendingMessages)) {
        lastId = '-'
        await new Promise((resolve) => setTimeout(resolve, 1000))
        continue
      } else {
        lastId = pendingMessages[pendingMessages.length - 1][0]
      }

      const staleMessageData = pendingMessages.find(
        (item) => item[2] > this.timeout
      )

      if (!staleMessageData) {
        continue
      }

      try {
        const messageMetadata = new PendingMessageMetadata(
          staleMessageData[0],
          staleMessageData[1],
          staleMessageData[2],
          staleMessageData[3]
        )
        const res = (await this.secondaryConnection.xclaim(
          this.stream,
          this.group,
          this.id,
          this.timeout,
          messageMetadata.id
        )) as unknown as [string, string[]][]

        if (res.length > 0) {
          const [[id, kv]] = res
          lastId = id
          this.logger.info(
            `transfer stale message[${id}] to consumer[${this.id}]`
          )
          const data = JSON.parse(kv[1])
          const msg = new Message(id, data)
          await this.processMessage(msg)
        }
      } catch (err) {
        this.logger.error('failed to claim stale messages: %o', err)
      }
    }
  }
  private async processMessage(message: Message) {
    try {
      await this.taskPool.add(() => this.onMessage(message))
      this.emit('success', message)
    } catch (err) {
      this.emit('error', err, message)
    } finally {
      await this.ackMessage(this.stream, this.group, message.id)
    }
  }
  private async ackMessage(stream: string, group: string, messageId: string) {
    try {
      await this.secondaryConnection.xack(stream, group, messageId)
      this.logger.info('message acknowledged:%s', messageId)
    } catch (err) {
      this.logger.error(`failed to ack the message[${messageId}], err:%o`, err)
    }
  }
  private async consumeMessages(rawMessages: [string, string[]][]) {
    if (_.isEmpty(rawMessages)) {
      return
    }
    const messages = rawMessages.map((msg) => {
      try {
        const [id, dataArray] = msg
        const [, payloadStr] = dataArray
        const payload: IPayload = JSON.parse(payloadStr)
        this.logger.debug('raw message:%o', msg)
        return new Message(id, payload)
      } catch (err) {
        this.logger.error('failed to parse message payload:%o', msg)
        return null
      }
    })
    const tasks = messages.map(async (message) => {
      if (message) {
        return this.processMessage(message)
      }
    })
    await Promise.allSettled(tasks)
  }
}

const onMessage = async (message: Message) => {
  // eslint-disable-next-line no-console
  console.log('handle message:', message.body)
}

const consumer = new Consumer(uuid(), 'cater', 'test', onMessage, {
  connection: { host: '192.168.1.200' }
})

consumer.start()
