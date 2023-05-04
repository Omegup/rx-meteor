import { Meteor, Subscription } from 'meteor/meteor'
import { Mongo } from 'meteor/mongo'
import { Observable, Subject, Subscriber } from 'rxjs'
import { finalize } from 'rxjs/operators'
import { omit } from 'rx-meteor-util'
import isEqual from 'fast-deep-equal'

var log: (args: () => any[]) => void = () => { }

export const setLog = (enabled: 0 | 1) => log = enabled ? args => console.log(...args()) : () => { }

export const logSub = (pre: string, sub: Subscription | null, args: () => any[]) => {
  if (sub) log(() => [pre, sub.connection.id.slice(0, 3), sub._subscriptionId.slice(0, 3), sub._subscriptionHandle.slice(0, 3), ...args()])
  else log(() => [pre, sub, ...args()])
}

export const startsWith = (s: string): Mongo.FieldExpression<string | null> => {
  if (!s) return { $ne: null }
  return { $gte: s, $lt: s.slice(0, -1) + String.fromCharCode(s.slice(-1).charCodeAt(0) + 1) }
}

// a count-ref watched set of ids of Src records referenced by Dst records
export type Watched<Src extends { _id?: unknown }, Dst extends { _id?: unknown }, srcId extends keyof Src = '_id', dstId extends keyof Dst = '_id', SID = Src[srcId], ID = Dst[dstId]> = {
  set: Map<SID, ID[]>
  set2: Map<ID, Set<SID>>
  obs: Subject<[SID, ID[]][]>
}

export const init = <Src extends { _id?: unknown }, Dst extends { _id?: unknown }, srcId extends keyof Src = '_id', dstId extends keyof Dst = '_id', SID = Src[srcId], ID = Dst[dstId]>(
): Watched<Src, Dst, srcId, dstId, SID, ID> => ({ set: new Map, set2: new Map, obs: new Subject, })

export const _addId = <Src extends { _id?: unknown }, Dst extends { _id?: unknown }, srcId extends keyof Src = '_id', dstId extends keyof Dst = '_id', SID = Src[srcId], ID = Dst[dstId]>(
  list: Watched<Src, Dst, srcId, dstId, SID, ID>, id: SID, vID: ID) => {
  const current = list.set.get(id)
  if (current?.find(eID => eID === vID)) throw new Error('entity already added')
  list.set.set(id, [...(list.set.get(id) ?? []), vID])
}
export const addIds = <Src extends { _id?: unknown }, Dst extends { _id?: unknown }, srcId extends keyof Src = '_id', dstId extends keyof Dst = '_id', SID = Src[srcId], ID = Dst[dstId]>(
  list: Watched<Src, Dst, srcId, dstId, SID, ID>, ids: Iterable<SID>, vID: ID) => {
  const idSet = new Set(ids)
    ;[...idSet].forEach(id => _addId(list, id, vID))
  list.set2.set(vID, idSet)
  list.obs.next([...list.set.entries()])
}

export const _removeId = <Src extends { _id?: unknown }, Dst extends { _id?: unknown }, srcId extends keyof Src = '_id', dstId extends keyof Dst = '_id', SID = Src[srcId], ID = Dst[dstId]>(
  list: Watched<Src, Dst, srcId, dstId, SID, ID>, id: SID, vID: ID) => {
  const current = list.set.get(id)
  if (!current) throw new Error(`${id} not found`)
  const index = current?.findIndex(eID => eID === vID)
  if (index === -1) throw new Error('entity not found')
  current.splice(index, 1)
  if (!current.length) list.set.delete(id)
}
export const removeIds = <Src extends { _id?: unknown }, Dst extends { _id?: unknown }, srcId extends keyof Src = '_id', dstId extends keyof Dst = '_id', SID = Src[srcId], ID = Dst[dstId]>(
  list: Watched<Src, Dst, srcId, dstId, SID, ID>, vID: ID, { optional }: { optional?: true } = {}) => {
  const currentIds = list.set2.get(vID)
  if (!currentIds && optional) { return }
  [...currentIds || []].forEach(id => _removeId(list, id, vID))
  list.set2.delete(vID)
  list.obs.next([...list.set.entries()])
}

export const updateIds = <Src extends { _id?: unknown }, Dst extends { _id?: unknown }, srcId extends keyof Src = '_id', dstId extends keyof Dst = '_id', SID = Src[srcId], ID = Dst[dstId]>(
  list: Watched<Src, Dst, srcId, dstId, SID, ID>, ids: Iterable<SID>, vID: ID) => {
  log(() => ['++++', ids, vID])
  const idSet = new Set(ids)
  const currentIds = list.set2.get(vID)
  const newIds = [...idSet].filter(id => !currentIds?.has(id))
  newIds.forEach(id => _addId(list, id, vID))
  return () => {
    const oldIds = [...currentIds ?? []].filter(id => !idSet.has(id))
    oldIds.forEach(id => _removeId(list, id, vID))
    list.set2.set(vID, idSet)
    //if (oldIds.length || newIds.length) 
    list.obs.next([...list.set.entries()])
  }
}


export const getMap = <K, V>(map: Map<K, Exclude<V, undefined>>) => (k: K, or: () => Exclude<V, undefined>) => {
  let v = map.get(k) ?? or()
  map.set(k, v)
  return v
}
export const changesOn = <T>(base: Partial<T>, changed: Partial<T>) => {
  const newEntries = Object.entries(changed) as [keyof T, any][]
  return Object.fromEntries(Object.keys(base).filter(k => !(k in changed)).map(k => [k]).concat(
    newEntries.filter(([k, v]) => !isEqual(base[k], v)))) as Partial<T>
}

type Cursor<T> = Pick<Mongo.Cursor<T>, 'observeChanges' | '_getCollectionName'>

type Event<T> = { event: 'added' | 'changed' | 'removed', id: string, colName: string, fields?: Partial<T> }
export interface ObserveChangesCallbacks<T> {
  (events: Event<T>[]): void
}

const getId = ((m = new WeakMap<object, number>(), id = 0) => (x: object) => {
  if (m.has(x)) return m.get(x)
  m.set(x, ++id)
  return id
})()


const nSymb = Symbol('n'), iSymb = Symbol('i'), upSymb = Symbol('update')
declare module 'meteor/meteor' {
  interface Subscription {
    [nSymb]?: number
    [iSymb]?: number
    [upSymb]?: boolean
  }
}
export const inc = (sub: Subscription) => {
  logSub('inc', sub, () => [sub[iSymb], sub[nSymb]])
  sub[nSymb] = (sub[nSymb] ?? 0) + 1
}

export const dec = (sub: Subscription, rm?: boolean) => {
  logSub('dec', sub, () => [sub[iSymb], sub[nSymb]])
  if (!(sub[nSymb] = (sub[nSymb] ?? 0) - 1) && sub[upSymb]) {
    sub[upSymb] = false
    if (rm) {
      logSub('rm', sub, () => [sub[iSymb]])
      sub.removed('$notif', sub._subscriptionId)
    } else if (sub[iSymb]) {
      sub[iSymb]!++
      logSub('change', sub, () => [sub[iSymb]])
      sub.changed('$notif', sub._subscriptionId, { v: sub[iSymb] })
    } else {
      sub[iSymb] = 1
      logSub('add', sub, () => [sub[iSymb]])
      sub.added('$notif', sub._subscriptionId, { v: sub[iSymb] })
    }
  }
  logSub('??', sub, () => [sub[nSymb]])
}

export const observeChanges = <T>(cursors$: Observable<Cursor<T>[]>, cb: ObserveChangesCallbacks<T>, sub: Subscription) => {
  const collectionsIds = new Map<string, Map<string, [Partial<T>, number]>>()
  const handles = new Map<Cursor<T>, Meteor.LiveQueryHandle>()
  const addedIds = new Map<Cursor<T>, Set<string>>()
  let oldCursors: Cursor<T>[] = []
  const cleanUp = (cursor: Cursor<T>) => {
    inc(sub)
    log(() => ['handles remove', getId(cursor)])
    const events: Event<T>[] = []
    const colName = cursor._getCollectionName(), ids = collectionsIds.get(colName)!
    const clone = [...(addedIds.get(cursor) ?? [])]
    handles.get(cursor)?.stop()
    clone.forEach(id => {
      const old = ids.get(id)!
      // this line is only for prod
      if (!old) {
        console.error(`unexpected error old ${id} is undefined for cleaning`)
        return;
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
    cb(events)
    dec(sub)
  }
  const subscriber = new Subscriber<Pick<Mongo.Cursor<T, T>, 'observeChanges' | '_getCollectionName'>[]>(cursors => {
    try {
      inc(sub)
      log(() => ['starting', cursors.map(v => [v._getCollectionName(), getId(v)])])
      const tasks: (() => Event<T> | void)[] = []
      let run = (task: () => Event<T> | void): void => void tasks.push(task)
      cursors.filter(cursor => !handles.has(cursor)).map(
        cursor => [cursor, getMap(collectionsIds)(cursor._getCollectionName(), () => new Map), getMap(addedIds)(cursor, () => new Set)] as const
      ).forEach(([cursor, ids, addedIds]) => {
        const colName = cursor._getCollectionName()
        log(() => ['handles set', getId(cursor), colName])
        const handle = cursor.observeChanges({
          added: (id, fields) => {
            inc(sub)
            addedIds.add(id)
            const old = ids.get(id)
            if (old) {
              ids.set(id, [fields, old[1] + 1])
              run(() => {
                const changes = changesOn(old[0], fields)
                log(() => ['--', id, Object.keys(fields), Object.keys(old[0]), Object.keys(changes)])
                if (Object.keys(changes).length) return { event: 'changed', id, colName, fields: changes }
              })
            } else {
              ids.set(id, [fields, 1])
              run(() => ({ event: 'added', id, colName, fields }))
            }
          },
          changed: (id, fields) => {
            if (subscriber.closed) return
            log(() => ['++++++++++++++', id, cursor._getCollectionName()])
            inc(sub)
            const old = ids.get(id)
            if (!old) {
              throw new Error(`unexpected error old ${id} is undefined for changed`)
            }
            const v = merge(old[0], fields)
            ids.set(id, [v, old[1]])
            run(() => {
              log(() => ['||||||||||||||', id, cursor._getCollectionName()])
              return { event: 'changed', id, colName, fields }
            })
          },
          removed: (id) => {
            if (subscriber.closed) return
            addedIds.delete(id)
            const old = ids.get(id)
            if (!old) {
              throw new Error(`unexpected error old ${id} is undefined for removed`)
            }
            // only for prod, old should always be defined
            if (old[1] > 1) ids.set(id, [old[0], old[1] - 1])
            else {
              inc(sub)
              ids.delete(id)
              run(() => ({ event: 'removed', id, colName }))
            }
          },
        })
        handles.set(cursor, handle)
      })
      const copy = oldCursors
      oldCursors = cursors
      copy.filter(cursor => !cursors.find(c => c === cursor)).forEach(cleanUp)
      const exec = (task: () => Event<T> | void) => {
        try {
          if (subscriber.closed) return
          return task()
        } catch { }
      }
      const notify = (t = tasks) => {
        const events = t.map(exec)
        const filtered = events.filter((x): x is Exclude<typeof x, void> => x !== undefined)
        if (filtered.length) cb(filtered)
        if (filtered.length !== events.length) logSub(']]', sub, () => ['why', events])
        events.forEach(() => dec(sub))
      }
      run = task => notify([task])
      notify()
    } catch (e) {
      console.error('error')
      console.error(e)
    } finally {
      dec(sub)
    }
  }, e => {
    console.error('error')
    console.error(e)
  })
  return cursors$.pipe(finalize(() => {
    const copy = oldCursors
    oldCursors = []
    copy.forEach(cleanUp)
  })).subscribe(subscriber)
}
type Replace<T, V, C> = (T extends V ? void : never) | { v?: Partial<V> | 'ignore', ctx?: C } & (T extends V ? {} : { v: Partial<V> | 'ignore' })

interface AddedArgs<T> { id: string, colName: string, fields: Partial<T> }
interface UpdatedArgs<T, V> extends AddedArgs<T> { old: Store<T, V>, updated: Partial<T> }

export type CB<T, C = unknown, V = T> = {
  added?: (args: AddedArgs<T>) => Replace<T, V, C>,
  changed?: (args: UpdatedArgs<T, V>) => Replace<T, V, C>,
  removed?: (args: { id: string, colName: string, old: Store<T, V> }) => Replace<T, V, C>,
  afterAdded?: (args: { id: string, colName: string, fields: Store<T, V>, ctx: C | undefined }) => void,
  afterChanged?: (args: { id: string, colName: string, cleaned: Partial<V>, old: Store<T, V>, updated: Partial<V> | 'ignore', ctx: C | undefined }) => void,
  afterRemoved?: (args: { id: string, colName: string, fields: Store<T, V>, ctx: C | undefined }) => void,
  notif?: () => void,
} & ({ added: {}, changed: {} } | (T extends V ? {} : never))
export type Store<T, V> = { db: Partial<T> | 'ignore', col: Partial<V> | 'ignore' }

export function merge<T extends {}>(base: Partial<T>, changes: Partial<T>): Partial<T>
export function merge<T extends {}>(base: T, changes: Partial<T>): T
export function merge<T extends {}>(base: T, changes: T): T {
  const p = { ...base, ...changes } as T & object
  return omit(p, ...Object.entries(changes).filter(([, v]) => v === undefined).map(([k]) => k)) as typeof p
}

export const find = <T extends {}>(
  col: Mongo.Collection<T>, selector: Mongo.Selector<T> | string, mod: Mongo.Options<T> & { fields: Record<string, 0 | 1> }
) => col.find(selector, mod) // extractFrom(col.find(selector, mod), mod.fields)


/**
 * Watch an observable of cursors while applying a list of callbacks, multiplexing changes and managing versions
 * @param subscription the subscription to be published to
 * @param cur observable of cursors to be checkong on
 * @param callbacks the event handles
 * @param publish whether to publish the events to the underlying subscription
 * @returns an RxJS subscription
 */
export const watch = <T extends {}, C = unknown, V extends {} = T>(
  subscription: Subscription<V>,
  cur: Observable<Cursor<T>[]>,
  callbacks: CB<T, C, V>,
  publish = true
) => {
  const sub = publish ? subscription : null
  const map = new Map<string, Store<T, V>>()
  const mergeOrReuse = <V extends {}>(
    a: unknown, x: V, b: unknown, y: V, merged: V
  ) => (a === x) && (b === y) ? merged : merge<V>(x, y)
  const notifyChange = (id: string, colName: string, changed: 'ignore' | Partial<V>, dbupdated: 'ignore' | Partial<T>, fields: Partial<T>, ctx?: C) => {
    const old = map.get(id)!, { col: oldcol, db: olddb } = old
    const colupdated = changed === 'ignore' ? oldcol : oldcol === 'ignore' ? changed : mergeOrReuse(olddb, oldcol, fields, changed, dbupdated as {} as Partial<V>)
    const entry: Store<T, V> = { db: dbupdated, col: colupdated }
    map.set(id, entry)
    if (changed === 'ignore') return
    if (oldcol === 'ignore') {
      if (sub) sub[upSymb] = true
      sub?.added(colName, id, changed)
      callbacks.afterAdded?.({ id, colName, fields: entry, ctx })
      return
    }
    const cleaned = colupdated !== dbupdated as {} as Partial<V> && colupdated !== 'ignore' ? changesOn(oldcol, changed) : changed
    if (sub) sub[upSymb] = true
    sub?.changed(colName, id, cleaned)
    callbacks.afterChanged?.({ id, colName, cleaned, old, updated: colupdated, ctx })
    logSub('~', subscription, () => [colName, id, fields, cleaned, Object.keys(cleaned)])
  }
  return observeChanges(cur, events => {
    const tasks = events.map(({ event, id, colName, fields }) => ({
      added() {
        const { v: replace, ctx } = callbacks.added?.({ id, colName, fields: fields! }) || {}
        const added = replace || fields as {} as Partial<V>
        const entry = { db: fields!, col: added }
        map.set(id, entry)
        return () => {
          if (added === 'ignore') return
          if (sub) sub[upSymb] = true
          sub?.added(colName, id, added)
          logSub('+', subscription, () => [colName, id])
          callbacks.afterAdded?.({ id, colName, fields: entry, ctx })
        }
      },
      changed() {
        const old = map.get(id)!
        const dbupdated = merge(old.db as Partial<T>, fields!)
        const { v: replace, ctx } = callbacks.changed?.({ id, colName, fields: fields!, old, updated: dbupdated }) || {}
        const changed = replace || fields as {} as Partial<V>
        return () => {
          notifyChange(id, colName, changed, dbupdated, fields!, ctx)
        }
      },
      removed() {
        const { v: res, ctx } = callbacks.removed?.({ id, colName, old: map.get(id)! }) || {}
        return () => {
          if (!res) {
            // TODO check why Error: Removed nonexistent document sometimes happen
            if (sub) sub[upSymb] = true
            sub?.removed(colName, id)
            callbacks.afterRemoved?.({ id, colName, fields: map.get(id)!, ctx })
            map.delete(id)
            logSub('-', subscription, () => [colName, id])
          } else {
            notifyChange(id, colName, res, 'ignore', {}, ctx)
          }
        }
      }
    })[event]())
    tasks.forEach(f => f())
    callbacks.notif?.()
  }, subscription)
}


