import { Checkbox, Form, Switch, type FormInstance } from '../../ui'

export const PRESET_GROUP_OPTIONS = [
  { value: 'select', label: '节点选择' },
  { value: 'auto', label: '自动选择' },
  { value: 'fallback', label: '故障转移' },
  { value: 'apple', label: '苹果服务' },
  { value: 'google', label: '谷歌服务' },
  { value: 'ai', label: 'AI 服务' },
  { value: 'telegram', label: '电报消息' },
  { value: 'final', label: '漏网之鱼' },
]

const known = new Set(PRESET_GROUP_OPTIONS.map((option) => option.value))

/** Form values for a stored group list. Empty means the node joins every rule. */
export function presetFormValues(groups?: string[]) {
  const selected = (groups ?? []).filter((group) => known.has(group))
  return {
    preset_all: selected.length === 0,
    preset_groups: selected,
  }
}

/** Payload stored on the node. Empty keeps the node in every built-in rule. */
export function presetGroupsFromForm(all: boolean, groups?: string[]) {
  if (all) return []
  return (groups ?? []).filter((group) => known.has(group))
}

/** Short label for lists. An empty selection is the historical "every rule" state. */
export function presetGroupSummary(groups?: string[]) {
  const selected = (groups ?? []).filter((group) => known.has(group))
  if (selected.length === 0) return '全部分流'
  return PRESET_GROUP_OPTIONS.filter((option) => selected.includes(option.value)).map((option) => option.label).join('、')
}

export function PresetGroupFields({ form }: { form: FormInstance }) {
  const all = Form.useWatch('preset_all', form)
  const joinAll = all !== false
  return (
    <>
      <Form.Item
        name="preset_all"
        label="加入全部分流"
        valuePropName="checked"
        extra="关闭后，这个节点只出现在勾选的 Clash 内置分流里。"
        className="fm-full"
      >
        <Switch />
      </Form.Item>
      {joinAll ? null : (
        <Form.Item
          name="preset_groups"
          label="分流规则"
          className="fm-full"
          rules={[{
            validator: (_, value) => (
              Array.isArray(value) && value.length > 0
                ? Promise.resolve()
                : Promise.reject(new Error('请至少选择一个分流规则'))
            ),
          }]}
        >
          <Checkbox.Group options={PRESET_GROUP_OPTIONS} className="fm-chips" />
        </Form.Item>
      )}
    </>
  )
}
