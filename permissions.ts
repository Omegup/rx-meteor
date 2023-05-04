import { Meteor, Subscription } from 'meteor/meteor'
import { concat, NEVER, Observable, of } from 'rxjs'
import { distinctUntilChanged, map, switchMap } from 'rxjs/operators'
import { Mongo } from 'meteor/mongo'
import isEqual from 'fast-deep-equal'
import { observeCursor } from 'rx-meteor-util'

import { addIds, init, logSub, removeIds, updateIds, watch, Watched } from './watch'
import { lazy } from './utils/lazy'

interface RoleBase {
  _id?: string
  ownOnly?: true,
  call?: string,
  except?: string[],
  children: { _id: string }[],
}

interface RoleAssignment {
  role: { _id: string, },
  user: { _id: string, },
}

type Perms<Role, K extends string, types extends Record<K, {}>> = { [r in keyof Role]?: { [k in K]?: [keyof types[k], keyof Meteor.User] } }

const up = <T, V extends T>(x: Mongo.Selector<T>): Mongo.Selector<V> => x


export const makePermissions = <K extends string, Models extends Record<K, { _id?: string, createdBy?: string }>, Role extends RoleBase, Call extends Record<string, Partial<Role>>>(
  roles: Mongo.IdsCollection<Role>,
  roleAssignment: Mongo.Collection<RoleAssignment>,
  perms: Perms<Role, K, Models>
) => {

  type Tp<k extends keyof Call> = k extends `${infer U}.${string}` ? U & K : never

  const getRolesForUser = (userId: string) => roleAssignment.find({ user: { _id: userId } }).map(x => x.role._id)

  const checkCallMany = <k extends string & keyof Call>(
    call: k, eM: () => Models[Tp<k>][]) => {
    const e = lazy(() => {
      const [v, ...rest] = eM()
      if (rest.length) checkCallMany(call, () => rest)
      return v
    })
    const model = call.split('.', 1)[0] as Tp<k>
    const userId = Meteor.userId()
    if (userId === null) {
      throw new Meteor.Error('unexpected')
    }
    let pending = roles.find({ _id: { $in: getRolesForUser(userId) } }).fetch()
    const roleMap: Record<string, Role> = {}
    while (pending.length) {
      const role = pending.filter(
        r => [call, '__ALL__'].includes(r.call!) && !r.except?.includes(call)
      ).find(r => {
        if (r.ownOnly) {
          const u = e()['createdBy']
          if (u && u !== userId) return false
        }
        const user = lazy(() => Meteor.user())
        return Object.entries<Perms<Role, K, Models>, {}>(perms).every(([k, v]) => {
          if (!r[k] || !v?.[model]) return true
          const [modelField, userField] = v[model]!
          return [user()?.[userField]].flat().some(id => [e()[modelField]].flat().includes(id))
        })
      })
      if (role) return
      pending.forEach(x => roleMap[x._id!] = x)
      const children = pending.flatMap(r => r.children).map(c => c._id).filter(r => !roleMap[r])
      const roles_ = roles.find({ _id: { $in: children } }).fetch()
      roles_.forEach(x => Object.assign(x, pending.find(p => p.children.find(c => c._id === x._id)), x))
      pending = roles_
    }
    throw new Meteor.Error('unauthorized')
  }

  const checkCall = (call: string, handleParams?: (role: Role) => boolean) => {
    const uid = Meteor.userId()
    if (uid && !roles.find(up({
      _id: { $in: getRolesForUser(uid) },
      call: { $in: [call, '__ALL__'] },
      except: { $ne: call },
    })).fetch().find(r => handleParams?.(r) ?? true)) {
      throw new Meteor.Error('unauthorized')
    }
  }

  const recursiveRoles = (uid: string | null, roleIds: Watched<Role, { _id: string }>, sub: Subscription) => {
    const cursor = roleAssignment.find({ 'user._id': uid })

    logSub('°°°', sub, () => ['@@@'])

    const innerSubs = watch<Role, () => void>(sub, roleIds.obs.pipe(
      map(ids => ids.map(([id]) => id)),
      distinctUntilChanged(isEqual),
      map(ids => {
        return [roles.find({ _id: { $in: ids } })]
      }),
    ), {
      added: ({ id, fields: e }) => {
        const ids = (e.children ?? []).map(c => c._id)
        if (ids.length) addIds(roleIds, ids, id)
      },
      changed: ({ id, updated: e }) => {
        return { ctx: updateIds(roleIds, (e.children ?? []).map(c => c._id), id) }
      },
      afterChanged: ({ ctx }) => ctx?.(),
      afterRemoved: ({ id }) => removeIds(roleIds, id, { optional: true })
    }, false)

    const subs = watch<RoleAssignment, () => void>(sub, concat(of([cursor]), NEVER), {
      added: ({ id, fields: e }) => {
        addIds(roleIds, [e.role!._id], id)
      },
      changed: ({ id, updated: e }) => {
        return { ctx: updateIds(roleIds, e.user!._id === uid ? [e.role!._id] : [], id) }
      },
      afterChanged: ({ ctx }) => ctx?.(),
      afterRemoved: ({ id }) => removeIds(roleIds, id, { optional: true })
    }, false)
    subs.add(innerSubs)

    return subs
  }

  const getPubRoles = (model: string, sub: Subscription) => new Observable<Role[]>(subscriber => {

    const roleIds = init<Role, { _id: string }>()

    roleIds.obs.pipe(
      map(ids => ids.map(([id]) => id)),
      distinctUntilChanged(isEqual),
      switchMap(ids => observeCursor(roles.find(up({
        _id: { $in: ids },
        publish: { $in: [model, '__ALL__'] },
        except: { $ne: model },
      }))))).subscribe(subscriber)

    subscriber.add(recursiveRoles(Meteor.userId(), roleIds, sub))

  })

  return { getPubRoles, checkCall, checkCallMany }
}