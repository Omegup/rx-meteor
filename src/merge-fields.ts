import type { Mongo } from 'meteor/mongo'

import { id, omit, pick } from './util'

/**
 * Intersect a caller's field specifier with a restriction.
 *
 * Numeric entries are `0` (exclude) or `1` (include). A field stays included
 * only when the restriction includes it and the caller asked for it. `$meta`
 * and other non-numeric entries on the caller specifier are preserved.
 * An empty caller specifier means "no opinion": the restriction is used as-is.
 */
export const mergeFields = (
  optFields: Mongo.FieldSpecifier,
  restriction: Mongo.FieldSpecifier,
): Mongo.FieldSpecifier => {
  const restrictionValues = Object.values(restriction ?? {})
  const fieldsNumberKeys = Object.keys(optFields).filter(k => typeof optFields[k] === 'number')
  let fields: Mongo.FieldSpecifier = pick(optFields, ...fieldsNumberKeys)
  if (restrictionValues.length) {
    const values = Object.values(fields)
    const picks: string[] = []
    const omits: string[] = []
    for (const [field, spec] of Object.entries(restriction)) {
      if (typeof spec !== 'number') continue
      ;(spec && fields[field] ? picks : omits).push(field)
    }
    if (!values.length) fields = restriction
    else if (values.some(id)) fields = omit(pick(fields, ...picks), ...omits)
    else if (restrictionValues.some(id))
      fields = omit(pick(restriction, ...picks), ...Object.keys(fields))
    else fields = { ...fields, ...restriction }
    if (!Object.keys(fields).length) fields = { _id: 1 }
    const meta = omit(optFields, ...fieldsNumberKeys)
    return Object.values(fields).some(id) ? { ...fields, ...meta } : { ...meta, ...fields }
  }
  return optFields
}
