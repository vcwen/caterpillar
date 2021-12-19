import EventEmitter from 'events'

export class MessageEventEmitter extends EventEmitter {
  constructor() {
    super()
  }
  public on(event: 'success', listener: (message: Message) => void): this
  public on(
    event: 'error',
    listener: (error: unknown, message: Message) => void
  ): this
  public on(
    event: 'success' | 'error',
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    listener: (...args: any[]) => void
  ): this {
    super.on(event, listener)
    return this
  }

  public emit(event: 'success', message: Message): boolean
  public emit(event: 'error', error: unknown, message: Message): boolean
  public emit(event: 'success' | 'error', ...args: unknown[]): boolean {
    return super.emit(event, ...args)
  }
}

export interface IPayload {
  header: Record<string, unknown>
  body: Record<string, unknown>
}

export class PendingMessageMetadata {
  constructor(
    public id: string,
    public consumer: string,
    public timeElapsed: number,
    public deliverTimes: number
  ) {}
}

export class Message extends EventEmitter {
  public id: string
  public header: Record<string, unknown>
  public body: Record<string, unknown>
  public isCanceled = false
  constructor(id: string, payload: IPayload) {
    super()
    this.id = id
    this.header = payload.header
    this.body = payload.body
  }
  public on(event: 'cancel', listener: () => void): this {
    super.on(event, listener)
    return this
  }
  public emit(event: 'cancel'): boolean {
    this.isCanceled = true
    return super.emit(event)
  }
  public cancel() {
    this.emit('cancel')
  }
}
