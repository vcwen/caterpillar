import { TaskPool } from '../src/task-pool'

const sleep = (ms: number) => {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

const sleepAndError = (ms: number) => {
  return new Promise((resolve, reject) =>
    setTimeout(() => reject(new Error('timeout')), ms)
  )
}
describe('TaskPool', () => {
  it('should init', () => {
    const taskPool = new TaskPool()
    expect(taskPool).toBeInstanceOf(TaskPool)
  })
  it('should run the task immediately if there free executors', async () => {
    const taskPool = new TaskPool()
    const startTime = Date.now()
    await taskPool.add(() => {
      // console.log('task completed')
    })
    const endTime = Date.now()
    expect(endTime - startTime).toBeLessThan(50)
  })

  it("should run tasks immediately if there're free executors", async () => {
    const taskPool = new TaskPool()
    const startTime = Date.now()
    const tasks: (() => Promise<unknown>)[] = []
    for (let i = 0; i < 5; i++) {
      tasks.push(() => sleep(1000))
    }
    await Promise.all(tasks.map((task) => taskPool.add(task)))
    const endTime = Date.now()
    expect(endTime - startTime).toBeLessThan(1050)
  })
  it("should queue tasks  if there're no free executors", async () => {
    const taskPool = new TaskPool()
    const startTime = Date.now()
    const tasks: (() => Promise<unknown>)[] = []
    for (let i = 0; i < 6; i++) {
      if (i % 2) {
        tasks.push(() => sleepAndError(1000))
      } else {
        tasks.push(() => sleep(1000))
      }
    }
    await Promise.all(
      tasks.map((task) =>
        taskPool.add(task).catch(() => {
          // nothing
        })
      )
    )
    const endTime = Date.now()
    expect(endTime - startTime).toBeGreaterThan(2000)
  })
})
