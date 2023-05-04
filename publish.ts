import { Subscription } from 'meteor/meteor'
import { Mongo } from 'meteor/mongo'
import { Observable, Subscription as RxSubscription, Subject } from 'rxjs'
import { map, sample } from 'rxjs/operators'

import { addIds, CB, dec, inc, init, removeIds, updateIds, watch, Watched } from './watch'
import { Field, Watchers } from './types'

const tuple = <T extends any[]>(...args: T) => args
const get = (obj: any[], path: string[]): any[] => path.length ? get(obj.flatMap(o => o[path[0]]), path.slice(1)) : obj
const getField = (id: string, e: unknown[], field: string): string[] => (field === '_id' ? [id] : get(e, field.split('.'))).filter(x => x)

export const publish = <T extends { _id?: string }, M, Pub>(
  sub: Subscription,
  cursors$: Observable<Mongo.Cursor<T>[]>,
  watchers: Watchers<Pub>,
  binds: Record<string, Field<Pub, M>>,
  list?: [string, string] | null,
) => {
  const subs = new RxSubscription()
  subs.add(() => inc(sub))
  // localField -> Watched ( Any -> this )
  type W = Watched<{  _id: string; }, T, "_id", "_id", string, T["_id"]>
  const linkedModelsIds = Object.fromEntries<Record<string, W>>(Object.entries(binds)
    .map(([k]) => tuple(k, init<{ _id: string }, T>())))

  const notifier = new Subject<void>()
  Object.entries(linkedModelsIds).forEach(([name, list]) => {
    Object.entries(binds[name]).forEach(<k extends keyof Pub>([collection, a]: [k, undefined | [Pub[k], (string & keyof M[k & keyof M])?]]) => {
      const [params, path] = a!
      subs.add(watchers[collection]?.(sub, list!.obs.pipe(map(ids => ids.map(([id]) => id)), sample(notifier)), params, path))
    })
  })

  const cb: CB<T, () => void> = {
    notif: () => notifier.next(),
    added: ({ id, fields: e }: { id: string, colName: string, fields: Partial<T> }) => Object.entries(linkedModelsIds).forEach(([field, list]) => {
      // field is the local field (in T), ref is its value (same as the value of foreign field)
      const ref = getField(id, [e], field) // e[field]
      if (ref.length) addIds(list!, ref, id)
    }),
    changed: ({ id, updated: e }: { id: string, updated: Partial<T> }) => {
      const cbs: (() => void)[] = []
      Object.entries(linkedModelsIds).forEach(([field, list]) => {
        const ref = getField(id, [e], field)
        if (ref.length) cbs.push(updateIds(list!, ref, id))
      })
      return { ctx: () => cbs.forEach(cb => cb()) }
    },
    afterChanged: ({ ctx }) => ctx?.(),
    afterRemoved: ({ id }) => Object.values(linkedModelsIds).forEach(list => removeIds(list!, id, { optional: true }))
  }

  if (list) {
    const [listName, listId] = list
    cb.afterAdded = ({ id }) => {
      sub.added(listName, listId + id, { list: listId })
    }
    cb.removed = ({ id }) => {
      sub.removed(listName, listId + id)
    }
  }
  subs.add(watch<T, () => void>(sub, cursors$, cb, list !== null))
  subs.add(() => dec(sub, true))
  return subs
}
