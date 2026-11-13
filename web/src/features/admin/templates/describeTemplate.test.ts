import { describe, expect, it } from 'vitest'
import { describeTemplate } from './SubscriptionTemplates'

describe('describeTemplate', () => {
  it('summarizes an include-all template with exclusions', () => {
    expect(
      describeTemplate({
        include_all: true,
        rules: [{ field: 'source', match: 'equals', value: 'external', exclude: true }],
      }),
    ).toBe('全部节点；排除：来源等于 external')
  })

  it('joins include rules with 或', () => {
    expect(
      describeTemplate({
        include_all: false,
        rules: [
          { field: 'region', match: 'equals', value: 'HK', exclude: false },
          { field: 'protocol', match: 'contains', value: 'vless', exclude: false },
        ],
      }),
    ).toBe('地区等于 HK 或 协议包含 vless')
  })

  it('warns when nothing can match', () => {
    expect(describeTemplate({ include_all: false, rules: [] })).toContain('不匹配任何节点')
  })
})
