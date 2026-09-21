const { describe, it } = require('node:test')
const assert = require('node:assert/strict')
const { mergeFields } = require('../dist/merge-fields.js')

describe('mergeFields', () => {
  it('keeps fields that both sides include', () => {
    assert.deepEqual(
      mergeFields({ title: 1, authorId: 1, secret: 1 }, { title: 1, authorId: 1, secret: 0 }),
      { title: 1, authorId: 1 },
    )
  })

  it('uses the restriction when the caller sends no fields', () => {
    assert.deepEqual(mergeFields({}, { title: 1, secret: 0 }), { title: 1, secret: 0 })
  })

  it('returns the caller specifier when the restriction is empty', () => {
    assert.deepEqual(mergeFields({ title: 1 }, {}), { title: 1 })
  })
})
