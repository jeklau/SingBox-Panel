import { describe, expect, it } from 'vitest'
import { presetFormValues, presetGroupSummary, presetGroupsFromForm } from './presetGroups'

describe('preset group form values', () => {
  it('treats an empty selection as every rule', () => {
    expect(presetFormValues(undefined)).toEqual({ preset_all: true, preset_groups: [] })
    expect(presetFormValues([])).toEqual({ preset_all: true, preset_groups: [] })
    expect(presetGroupsFromForm(true, ['ai'])).toEqual([])
  })

  it('keeps an explicit subset and drops unknown keys', () => {
    expect(presetFormValues(['ai', 'nope', 'telegram'])).toEqual({
      preset_all: false,
      preset_groups: ['ai', 'telegram'],
    })
    expect(presetGroupsFromForm(false, ['ai', 'nope'])).toEqual(['ai'])
    expect(presetGroupSummary(['ai', 'telegram'])).toBe('AI 服务、电报消息')
    expect(presetGroupSummary([])).toBe('全部分流')
  })
})
