export const lazy = <T>(thunk?: () => T) => {
    let res: T
    return () => {
      if (!thunk) return res
      res = thunk()
      thunk = undefined
      return res
    };
  }
  