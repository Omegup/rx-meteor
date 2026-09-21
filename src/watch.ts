import { Meteor } from 'meteor/meteor'
import { Mongo } from 'meteor/mongo'

declare module 'meteor/mongo' {
  namespace Mongo {
    interface Cursor<T, U = T> {
      _getCollectionName(): string
    }
  }
}
import { Observable, Subject, Subscriber } from 'rxjs'
import { finalize } from 'rxjs/operators'
import isEqual from 'fast-deep-equal'

const omit = <T extends object>(object: T, ...paths: (keyof any)[]): T =>
  paths.reduce((o, p) => (delete o[p], o), { ...object })

export type PublishSub = {
  connection: { id: string }
  _subscriptionId: string
  _subscriptionHandle: string
  added(collection: string, id: string, fields: object): void
  changed(collection: string, id: string, fields: object): void
  removed(collection: string, id: string): void
  ready?: () => void
}

var log: (args: () => any[]) => void = () => {}

/** Turn cursor and publication tracing on (`1`) or off (`0`). Off by default. */
export const setLog = (enabled: 0 | 1) =>
  (log = enabled ? args => console.log(...args()) : () => {})

export const logSub = (pre: string, sub: PublishSub | null, args: () => any[]) => {
  if (sub)
    log(() => [
      pre,
      sub.connection.id.slice(0, 3),
      sub._subscriptionId.slice(0, 3),
      sub._subscriptionHandle.slice(0, 3),
      ...args(),
    ])
  else log(() => [pre, sub, ...args()])
}

export const startsWith = (s: string): Mongo.FieldExpression<string> => {
  if (!s) return { $ne: null }
  return { $gte: s, $lt: s.slice(0, -1) + String.fromCharCode(s.slice(-1).charCodeAt(0) + 1) }
}

// a count-ref watched set of ids of Src records referenced by Dst records
export type Watched<
  Src extends { _id?: unknown },
  Dst extends { _id?: unknown },
  srcId extends keyof Src = '_id',
  dstId extends keyof Dst = '_id',
  SID = Src[srcId],
  ID = Dst[dstId],
> = {
  set: Map<SID, ID[]>
  set2: Map<ID, Set<SID>>
  obs: Subject<[SID, ID[]][]>
  emitted: boolean
}

export const init = <
  Src extends { _id?: unknown },
  Dst extends { _id?: unknown },
  srcId extends keyof Src = '_id',
  dstId extends keyof Dst = '_id',
  SID = Src[srcId],
  ID = Dst[dstId],
>(): Watched<Src, Dst, srcId, dstId, SID, ID> => ({
  set: new Map(),
  set2: new Map(),
  obs: new Subject(),
  emitted: false,
})

export const _addId = <
  Src extends { _id?: unknown },
  Dst extends { _id?: unknown },
  srcId extends keyof Src = '_id',
  dstId extends keyof Dst = '_id',
  SID = Src[srcId],
  ID = Dst[dstId],
>(
  list: Watched<Src, Dst, srcId, dstId, SID, ID>,
  id: SID,
  vID: ID,
) => {
  const current = list.set.get(id)
  if (current?.find(eID => eID === vID)) throw new Error('entity already added')
  list.set.set(id, [...(list.set.get(id) ?? []), vID])
}
export const addIds = <
  Src extends { _id?: unknown },
  Dst extends { _id?: unknown },
  srcId extends keyof Src = '_id',
  dstId extends keyof Dst = '_id',
  SID = Src[srcId],
  ID = Dst[dstId],
>(
  list: Watched<Src, Dst, srcId, dstId, SID, ID>,
  ids: Iterable<SID>,
  vID: ID,
) => {
  const idSet = new Set(ids)
  ;[...idSet].forEach(id => _addId(list, id, vID))
  list.set2.set(vID, idSet)
  list.obs.next([...list.set.entries()])
  list.emitted = true
}

export const _removeId = <
  Src extends { _id?: unknown },
  Dst extends { _id?: unknown },
  srcId extends keyof Src = '_id',
  dstId extends keyof Dst = '_id',
  SID = Src[srcId],
  ID = Dst[dstId],
>(
  list: Watched<Src, Dst, srcId, dstId, SID, ID>,
  id: SID,
  vID: ID,
) => {
  const current = list.set.get(id)
  const index = current.findIndex(eID => eID === vID)
  if (index === -1) throw new Error('entity not found')
  current.splice(index, 1)
  if (!current.length) list.set.delete(id)
}
export const removeIds = <
  Src extends { _id?: unknown },
  Dst extends { _id?: unknown },
  srcId extends keyof Src = '_id',
  dstId extends keyof Dst = '_id',
  SID = Src[srcId],
  ID = Dst[dstId],
>(
  list: Watched<Src, Dst, srcId, dstId, SID, ID>,
  vID: ID,
  { optional }: { optional?: true } = {},
) => {
  const currentIds = list.set2.get(vID)
  if (!currentIds && optional) return
  ;[...currentIds].forEach(id => _removeId(list, id, vID))
  list.set2.delete(vID)
  list.obs.next([...list.set.entries()])
  list.emitted = true
}

export const updateIds = <
  Src extends { _id?: unknown },
  Dst extends { _id?: unknown },
  srcId extends keyof Src = '_id',
  dstId extends keyof Dst = '_id',
  SID = Src[srcId],
  ID = Dst[dstId],
>(
  list: Watched<Src, Dst, srcId, dstId, SID, ID>,
  ids: Iterable<SID>,
  vID: ID,
) => {
  log(() => ['++++', ids, vID])
  const idSet = new Set(ids)
  const currentIds = list.set2.get(vID)
  const newIds = [...idSet].filter(id => !currentIds?.has(id))
  newIds.forEach(id => _addId(list, id, vID))
  return () => {
    const oldIds = [...(currentIds ?? [])].filter(id => !idSet.has(id))
    oldIds.forEach(id => _removeId(list, id, vID))
    list.set2.set(vID, idSet)
    //if (oldIds.length || newIds.length)
    list.obs.next([...list.set.entries()])
    list.emitted = true
  }
}

export const getMap =
  <K, V>(map: Map<K, Exclude<V, undefined>>) =>
  (k: K, or: () => Exclude<V, undefined>) => {
    let v = map.get(k) ?? or()
    map.set(k, v)
    return v
  }
export const changesOn = <T>(base: Partial<T>, changed: Partial<T>, ignoreRemove = false) => {
  const newEntries = Object.entries(changed) as [keyof T, any][]
  return Object.fromEntries(
    (!ignoreRemove
      ? Object.keys(base)
          .filter(k => !(k in changed))
          .map(k => [k])
      : []
    ).concat(newEntries.filter(([k, v]) => !isEqual(base[k], v))),
  ) as Partial<T>
}

type Event<T> = {
  event: 'added' | 'changed' | 'removed'
  id: string
  colName: string
  fields?: Partial<T>
}
export interface ObserveChangesCallbacks<T> {
  (events: Event<T>[]): void
}

const getId = (
  (m = new WeakMap<object, number>(), id = 0) =>
  (x: object) => {
    if (m.has(x)) return m.get(x)
    m.set(x, ++id)
    return id
  }
)()

const nSymb = Symbol('n'),
  iSymb = Symbol('i'),
  upSymb = Symbol('update')

type TrackedSub = PublishSub & {
  [nSymb]?: number
  [iSymb]?: number
  [upSymb]?: boolean
}

export const inc = (sub: PublishSub) => {
  const tracked = sub as TrackedSub
  logSub('inc', sub, () => [tracked[iSymb], tracked[nSymb]])
  tracked[nSymb] = (tracked[nSymb] ?? 0) + 1
}

export const dec = (sub: PublishSub, rm?: boolean) => {
  const tracked = sub as TrackedSub
  logSub('dec', sub, () => [tracked[iSymb], tracked[nSymb]])
  if (!(tracked[nSymb] = (tracked[nSymb] ?? 0) - 1) && tracked[upSymb]) {
    tracked[upSymb] = false
    if (rm) {
      logSub('rm', sub, () => [tracked[iSymb]])
      sub.removed('$notif', sub._subscriptionId)
    } else if (tracked[iSymb]) {
      tracked[iSymb]++
      logSub('change', sub, () => [tracked[iSymb]])
      sub.changed('$notif', sub._subscriptionId, { v: tracked[iSymb] })
    } else {
      tracked[iSymb] = 1
      logSub('add', sub, () => [tracked[iSymb]])
      sub.added('$notif', sub._subscriptionId, { v: tracked[iSymb] })
    }
  }
  logSub('??', sub, () => [tracked[nSymb]])
}

export const observeChanges = <T>(
  prefix: string,
  cursors$: Observable<Mongo.Cursor<T>[]>,
  cb: ObserveChangesCallbacks<T>,
  sub: PublishSub,
) => {
  if (prefix) console.log('prefix', prefix)
  const collectionsIds = new Map<string, Map<string, [Partial<T>, number]>>()
  const handles = new Map<Mongo.Cursor<T>, Meteor.LiveQueryHandle>()
  const addedIds = new Map<Mongo.Cursor<T>, Set<string>>()
  let oldCursors: Mongo.Cursor<T>[] = []
  // Cursor batches must run one at a time: Meteor 3 observeChanges is async,
  // and a later sample can arrive before the previous observe has resolved.
  let queue = Promise.resolve()
  const enqueue = (job: () => Promise<void>) => {
    queue = queue.then(job, job)
  }
  const getName = (cursor: Mongo.Cursor<T>) => cursor._getCollectionName()
  const stopHandle = (handle: unknown) => {
    const invoke = (resolved: unknown) => {
      const stop = (resolved as { stop?: () => unknown } | null)?.stop
      if (typeof stop !== 'function') return
      try {
        const stopping = stop.call(resolved)
        if (stopping && typeof (stopping as PromiseLike<unknown>).then === 'function')
          void (stopping as PromiseLike<unknown>).then(undefined, err => {
            console.error('error')
            console.error(err)
          })
      } catch (err) {
        console.error('error')
        console.error(err)
      }
    }
    if (handle != null && typeof (handle as PromiseLike<unknown>).then === 'function')
      void (handle as PromiseLike<unknown>).then(invoke, err => {
        console.error('error')
        console.error(err)
      })
    else invoke(handle)
  }
  const cleanUp = (cursor: Mongo.Cursor<T>, events: Event<T>[]) => {
    inc(sub)
    log(() => ['handles remove', getId(cursor)])
    const colName = getName(cursor),
      ids = collectionsIds.get(colName)!
    const clone = [...(addedIds.get(cursor) ?? [])]
    stopHandle(handles.get(cursor))
    clone.forEach(id => {
      const old = ids.get(id)!
      // this line is only for prod
      if (!old) {
        console.error(`unexpected error old ${id} is undefined for cleaning`)
        return
      }
      const [data, n] = old
      if (n > 1) ids.set(id, [data, n - 1])
      else {
        ids.delete(id)
        events.push({ event: 'removed', id, colName })
      }
    })
    addedIds.delete(cursor)
    handles.delete(cursor)
    dec(sub)
  }
  const subscriber = new Subscriber<Mongo.Cursor<T>[]>(
    cursors => {
      inc(sub)
      enqueue(async () => {
      try {
        log(() => ['starting', cursors.map(v => [getName(v), getId(v)])])
        const tasks: (() => Event<T> | void)[] = []
        let run = (task: () => Event<T> | void): void => void tasks.push(task)
        for (const [cursor, ids, seen] of cursors
          .filter(cursor => !handles.has(cursor))
          .map(
            cursor =>
              [
                cursor,
                getMap(collectionsIds)(getName(cursor), () => new Map()),
                getMap(addedIds)(cursor, () => new Set()),
              ] as const,
          )) {
            const colName = getName(cursor)
            log(() => ['handles set', getId(cursor), colName])
            const handle = await cursor.observeChanges({
              added: (i, fields) => {
                const id = prefix + i
                fields = rmTimestamp(fields)
                inc(sub)
                seen.add(id)
                const old = ids.get(id)
                if (old) {
                  ids.set(id, [fields, old[1] + 1])
                  run((): Event<T> => {
                    const changes = changesOn(old[0], fields, true)
                    log(() => [
                      '--',
                      id,
                      Object.keys(fields),
                      Object.keys(old[0]),
                      Object.keys(changes),
                    ])
                    if (Object.keys(changes).length)
                      return { event: 'changed', id, colName, fields: changes }
                  })
                } else {
                  ids.set(id, [fields, 1])
                  run(() => ({ event: 'added', id, colName, fields }))
                }
              },
              changed: (i, fields) => {
                const id = prefix + i
                if (subscriber.closed) return
                fields = rmTimestamp(fields)
                log(() => ['++++++++++++++', id, getName(cursor)])
                inc(sub)
                const old = ids.get(id)
                // only for prod, old should always be defined
                if (!old) {
                  console.error(`unexpected error old ${id} is undefined for changed`)
                }
                const v = !old ? fields : merge(old[0], fields)
                ids.set(id, [v, old?.[1]])
                run(() => {
                  log(() => ['||||||||||||||', id, getName(cursor)])
                  return { event: 'changed', id, colName, fields }
                })
              },
              removed: i => {
                if (subscriber.closed) return
                const id = prefix + i
                seen.delete(id)
                const old = ids.get(id)
                if (!old) {
                  console.error(`unexpected error old ${id} is undefined for removed`)
                }
                // only for prod, old should always be defined
                if (old?.[1] > 1) ids.set(id, [old[0], old[1] - 1])
                else {
                  inc(sub)
                  ids.delete(id)
                  run(() => ({ event: 'removed', id, colName }))
                }
              },
            })
            handles.set(cursor, handle)
          }
        const copy = oldCursors
        oldCursors = cursors
        const old: Event<T>[] = []
        copy.filter(cursor => !cursors.find(c => c === cursor)).forEach(cursor => cleanUp(cursor, old))
        const exec = (task: () => Event<T> | void) => {
          try {
            if (subscriber.closed) return
            return task()
          } catch {}
        }
        const notify = (events: (Event<T> | void)[]) => {
          const filtered = events.filter((x): x is Exclude<typeof x, void> => x !== undefined)
          cb(filtered)
          if (filtered.length !== events.length) logSub(']]', sub, () => ['why', events])
          events.forEach(() => dec(sub))
        }
        run = task => notify([exec(task)])
        const events = [...old, ...tasks.map(exec)]
        notify(events)
      } catch (e) {
        console.error('error')
        console.error(e)
        for (const cursor of handles.keys()) {
          if (!oldCursors.includes(cursor)) oldCursors.push(cursor)
        }
      } finally {
        dec(sub)
      }
      })
    },
    e => {
      console.error('error')
      console.error(e)
    },
  )
  return cursors$
    .pipe(
      finalize(() => {
        enqueue(async () => {
          const copy = oldCursors
          oldCursors = []
          const old: Event<T>[] = []
          copy.forEach(cursor => cleanUp(cursor, old))
          if (old.length) cb(old)
        })
      }),
    )
    .subscribe(subscriber)
}
type Replace<V, C> = void | { v?: Partial<V> | 'ignore'; ctx?: C }

export type CB<T, C = unknown> = {
  added?: (args: { id: string; colName: string; fields: Partial<T> }) => Replace<T, C>
  changed?: (args: {
    id: string
    colName: string
    fields: Partial<T>
    old: Store<T>
    updated: Partial<T>
  }) => Replace<T, C>
  removed?: (args: { id: string; colName: string; old: Store<T> }) => Replace<T, C>
  afterAdded?: (args: {
    id: string
    colName: string
    fields: Store<T>
    ctx: C | undefined
  }) => void
  afterChanged?: (args: {
    id: string
    colName: string
    cleaned: Partial<T>
    old: Store<T>
    updated: Partial<T> | 'ignore'
    ctx: C | undefined
  }) => void
  afterRemoved?: (args: {
    id: string
    colName: string
    fields: Store<T>
    ctx: C | undefined
  }) => void
  notif?: () => void
}

export type Store<T, V = T> = { db: Partial<T> | 'ignore'; col: Partial<V> | 'ignore' }

export function merge<T>(base: Partial<T>, changes: Partial<T>): Partial<T>
export function merge<T>(base: T, changes: Partial<T>): T
export function merge<T>(base: T, changes: T): T {
  const p = { ...base, ...changes } as T & object
  return omit(
    p,
    ...Object.entries(changes)
      .filter(([, v]) => v === undefined)
      .map(([k]) => k),
  ) as typeof p
}

const isMongoTimestamp = (v: unknown): v is { _bsontype: 'Timestamp' } =>
  typeof v === 'object' && v !== null && (v as { _bsontype?: string })._bsontype === 'Timestamp'

export const rmTimestamp = <T extends object>(x: T): Partial<T> =>
  Object.fromEntries(Object.entries(x).filter(([, v]) => !isMongoTimestamp(v))) as Partial<T>

/**
 * Observe an `Observable` of cursors and drive a Meteor subscription.
 *
 * Each emission replaces the previous cursor set. Cursors that are still in the
 * array keep their observe handle. Meteor 3 `observeChanges` returns a promise
 * and delivers the initial `added` callbacks before that promise resolves, so
 * batches are awaited and run one at a time.
 *
 * `publish` defaults to true and writes `added` / `changed` / `removed` on
 * `subscription`. Set it to false to keep the callbacks without DDP traffic.
 * Callbacks may replace the fields sent to the client (`{ v }`) or skip the
 * document (`{ v: 'ignore' }`). Return `{ ctx }` to pass a value to the matching
 * `after*` callback.
 */
export const watch = <T, C = unknown>(
  subscription: PublishSub,
  cur: Observable<Mongo.Cursor<T>[]>,
  callbacks: CB<T, C>,
  publish = true,
  prefix = '',
) => {
  type V = T
  const sub = publish ? (subscription as TrackedSub) : null
  const notifySub = (
    action: 'changed' | 'added',
    colName: string,
    id: string,
    fields: Partial<V>,
  ) => sub?.[action](colName, id, fields)
  const map = new Map<string, Store<T, V>>()
  const mergeOrReuse = <V>(a: unknown, x: V, b: unknown, y: V, merged: V) =>
    a === x && b === y ? merged : merge(x, y)
  const notifyChange = (
    id: string,
    colName: string,
    changed: 'ignore' | Partial<V>,
    dbupdated: 'ignore' | Partial<T>,
    fields: Partial<T>,
    ctx?: C,
  ) => {
    const old = map.get(id),
      { col: oldcol, db: olddb } = old
    const colupdated =
      changed === 'ignore'
        ? oldcol
        : oldcol === 'ignore'
          ? changed
          : mergeOrReuse(olddb, oldcol, fields, changed, dbupdated as {} as Partial<V>)
    const entry: Store<T, V> = { db: dbupdated, col: colupdated }
    map.set(id, entry)
    if (changed === 'ignore') return
    if (oldcol === 'ignore') {
      if (sub) sub[upSymb] = true
      sub?.added(colName, id, changed)
      callbacks.afterAdded?.({ id, colName, fields: entry, ctx })
      return
    }
    const cleaned: Partial<T> =
      colupdated !== (dbupdated as {} as Partial<V>) && colupdated !== 'ignore'
        ? changesOn(oldcol, changed)
        : changed
    if (sub) sub[upSymb] = true
    notifySub('changed', colName, id, cleaned)
    callbacks.afterChanged?.({ id, colName, cleaned, old, updated: colupdated, ctx })
    logSub('~', subscription, () => [colName, id, fields, cleaned, Object.keys(cleaned)])
  }
  return observeChanges(
    prefix,
    cur,
    events => {
      const tasks = events.map(({ event, id, colName, fields }) =>
        ({
          added() {
            const { v: replace, ctx } = callbacks.added?.({ id, colName, fields }) || {}
            const added = replace || (fields as {} as Partial<V>)
            const entry = { db: fields, col: added }
            map.set(id, entry)
            return () => {
              if (added === 'ignore') return
              if (sub) sub[upSymb] = true
              notifySub('added', colName, id, added)
              logSub('+', subscription, () => [colName, id])
              callbacks.afterAdded?.({ id, colName, fields: entry, ctx })
            }
          },
          changed() {
            const old = map.get(id)
            const dbupdated = merge(old.db as Partial<T>, fields)
            const { v: replace, ctx } =
              callbacks.changed?.({ id, colName, fields, old, updated: dbupdated }) || {}
            const changed = replace || (fields as {} as Partial<V>)
            return () => {
              notifyChange(id, colName, changed, dbupdated, fields, ctx)
            }
          },
          removed() {
            const { v: res, ctx } = callbacks.removed?.({ id, colName, old: map.get(id) }) || {}
            return () => {
              try {
                if (!res) {
                  if (sub) sub[upSymb] = true
                  sub?.removed(colName, id)
                  callbacks.afterRemoved?.({ id, colName, fields: map.get(id), ctx })
                  map.delete(id)
                  logSub('-', subscription, () => [colName, id])
                } else {
                  notifyChange(id, colName, res, 'ignore', {}, ctx)
                }
              } catch {}
            }
          },
        })[event](),
      )
      tasks.forEach(f => f())
      callbacks.notif?.()
    },
    subscription,
  )
}
