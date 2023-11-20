function itAsyncOnMacroTaskSync<T>(it: AsyncIterator<T>, consumer: (value: T) => Promise<void>, resolve: () => void, reject: (reason?: any) => void) {
  it.next()
    .then(({done, value}) => {
      if (done) resolve()
      else setImmediate(() => consumer(value)
        .then(() => itAsyncOnMacroTaskSync(it, consumer, resolve, reject))
        .catch(reject),
      )
    })
    .catch(reject)
}

export default function forAwaitOnMacroTaskQueue<T>(it: AsyncIterator<T>, consumer: (value: T) => Promise<void>): Promise<undefined> {
  return new Promise((resolve, reject) => {
    itAsyncOnMacroTaskSync(it, consumer, () => resolve(undefined), reject)
  })
}
