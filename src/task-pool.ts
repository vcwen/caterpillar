import EventEmitter from 'events'
import _ from 'lodash'

export class TaskExecutor<T = unknown> extends EventEmitter {
  constructor(private task: () => Promise<T> | T) {
    super()
  }

  public async execute() {
    try {
      const result = await this.task()
      this.emit('success', result)
    } catch (err) {
      this.emit('error', err)
    } finally {
      this.emit('complete')
    }
  }

  public on(event: 'complete', listener: () => void): this
  public on(event: 'success', listener: (result: T) => void): this
  public on(event: 'error', listener: (error: unknown) => void): this

  public on(
    event: 'complete' | 'success' | 'error',
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    listener: (...arg: any[]) => void
  ): this {
    super.on(event, listener)
    return this
  }

  public emit(event: 'complete'): boolean
  public emit(event: 'success', result: T): boolean
  public emit(event: 'error', error: unknown): boolean
  public emit(
    event: 'complete' | 'success' | 'error',
    data?: unknown
  ): boolean {
    return super.emit(event, data)
  }
}

export class TaskPool {
  private runningTaskNum = 0
  private tasks: TaskExecutor[] = []
  constructor(private concurrency: number = 5) {}
  public add<T>(task: () => Promise<T> | T): Promise<T> {
    return new Promise((resolve, reject) => {
      const taskExec = new TaskExecutor(task)
      taskExec.on('success', resolve)
      taskExec.on('error', reject)
      this.tasks.push(taskExec)
      this.scheduleTaskIfNecessary()
    })
  }

  private scheduleTaskIfNecessary() {
    if (this.runningTaskNum < this.concurrency && !_.isEmpty(this.tasks)) {
      const task = this.tasks.pop()
      if (task) {
        this.runningTaskNum += 1
        task.on('complete', () => this.onTaskComplete())
        task.execute()
      }
    }
  }
  private onTaskComplete() {
    this.runningTaskNum -= 1
    this.scheduleTaskIfNecessary()
  }
}
