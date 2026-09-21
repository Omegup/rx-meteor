export const omit = <T extends object, K extends (keyof any)[]>(
  object: T | null | undefined,
  ...paths: K
): Pick<T, Exclude<keyof T, K[number]>> =>
  object && paths.reduce((o, p) => (delete (o as any)[p], o), { ...object })

export const pick = <T extends object, U extends (keyof T)[]>(o: T, ...props: U): Pick<T, U[number]> =>
  Object.fromEntries(
    props.flatMap(p => (p in o ? [[p, o[p]]] : [])),
  ) as Pick<T, U[number]>

export const id = <T>(x: T) => x

export const getValueByKey = <T, V>(db: T, k: keyof T) => db[k] as unknown as V

export const subOptionals =
  <T>() =>
  <k extends keyof T>(x: { [p in k]?: T[p] }) =>
    x as { [p in keyof T]?: T[p] }
