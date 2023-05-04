export type Field<Pub, M> = { [k in keyof Pub]?: [Pub[k], (string & keyof M[k & keyof M])?] }
export type Publish<Pub, k extends keyof Pub> = (
    sub: import('meteor/meteor').Subscription,
    ids: import('rxjs').Observable<string[] | null>,
    params: Pub[k],
    path?: string
) => import('rxjs').Subscription

export type Watchers<Pub> = {
    [k in keyof Pub]: Publish<Pub, k>
}
export type Param<Pub, M, k extends keyof M> = [
    (import('meteor/mongo').Mongo.Selector<M[k]> | string)?,
    (import('meteor/mongo').Mongo.Options<M[k]> & { bind?: { [_: string]: Field<Pub, M> } & { [_ in keyof M[k]]?: Field<Pub, M> } })?,
    (string | null)?
]
export interface Model {
    _id?: string,
    deletedAt?: Date | null,
}
