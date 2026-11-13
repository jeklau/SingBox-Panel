import { describe, expect, it } from 'vitest'
import { bytesToGB, gbToBytes } from './UserDialogs'

describe('quota unit conversion', () => {
  it('converts GB to whole bytes', () => {
    expect(gbToBytes(1)).toBe(1073741824)
    expect(gbToBytes(0.5)).toBe(536870912)
  })

  it('treats empty, zero, negative and invalid input as unlimited', () => {
    expect(gbToBytes(0)).toBe(0)
    expect(gbToBytes(-3)).toBe(0)
    expect(gbToBytes(Number.NaN)).toBe(0)
  })

  it('round-trips a typical allowance', () => {
    expect(bytesToGB(gbToBytes(250))).toBe(250)
    expect(bytesToGB(0)).toBe(0)
  })
})
