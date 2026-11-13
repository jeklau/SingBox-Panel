import { useEffect, useRef, useState } from 'react'
import { Button, Input, Modal, Popconfirm, Select, Switch, Table, message } from '../../../ui'
import {
  createSubscriptionTemplate,
  deleteSubscriptionTemplate,
  errMsg,
  listSubscriptionTemplates,
  updateSubscriptionTemplate,
} from '../../../api'
import type { SubscriptionTemplate, SubscriptionTemplateBody, TemplateField, TemplateMatch, TemplateRule } from '../../../api'
import { RequestState } from '../../../components/RequestState'
import { PageHeader } from '../../../ui/Page'
import { MODAL_MD } from '../../../ui/modal'
import '../../../styles/users.css'

export const TEMPLATE_FIELDS: { value: TemplateField; label: string }[] = [
  { value: 'name', label: '节点名（服务器 + 标签）' },
  { value: 'tag', label: '入站标签' },
  { value: 'protocol', label: '协议' },
  { value: 'server', label: '服务器名' },
  { value: 'region', label: '地区' },
  { value: 'group', label: '分组（外部节点）' },
  { value: 'source', label: '来源（managed / external）' },
]

export const TEMPLATE_MATCHES: { value: TemplateMatch; label: string }[] = [
  { value: 'contains', label: '包含' },
  { value: 'equals', label: '等于' },
  { value: 'regex', label: '正则' },
]

export const emptyRule = (): TemplateRule => ({ field: 'region', match: 'equals', value: '', exclude: false })

/** Plain-language summary shown in the list. */
export function describeTemplate(t: Pick<SubscriptionTemplate, 'include_all' | 'rules'>): string {
  const label = (f: TemplateField) => TEMPLATE_FIELDS.find((x) => x.value === f)?.label.split('（')[0] ?? f
  const matchLabel = (m: TemplateMatch) => TEMPLATE_MATCHES.find((x) => x.value === m)?.label ?? m
  const part = (r: TemplateRule) => `${label(r.field)}${matchLabel(r.match)} ${r.value}`
  const includes = t.rules.filter((r) => !r.exclude).map(part)
  const excludes = t.rules.filter((r) => r.exclude).map(part)
  const head = t.include_all ? '全部节点' : includes.length ? includes.join(' 或 ') : '（无包含规则，不匹配任何节点）'
  return excludes.length ? `${head}；排除：${excludes.join(' 或 ')}` : head
}

function TemplateDialog({ template, onClose, onSaved }: {
  template?: SubscriptionTemplate
  onClose: () => void
  onSaved: () => void
}) {
  const [name, setName] = useState(template?.name ?? '')
  const [enabled, setEnabled] = useState(template?.enabled ?? true)
  const [includeAll, setIncludeAll] = useState(template?.include_all ?? false)
  const [rules, setRules] = useState<TemplateRule[]>(template?.rules ?? [])
  const [saving, setSaving] = useState(false)
  const lock = useRef(false)

  const setRule = (i: number, patch: Partial<TemplateRule>) =>
    setRules((cur) => cur.map((r, idx) => (idx === i ? { ...r, ...patch } : r)))

  const submit = async () => {
    if (lock.current) return
    if (!name.trim()) {
      message.error('请输入模板名称')
      return
    }
    if (rules.some((r) => !r.value.trim())) {
      message.error('规则的匹配值不能为空')
      return
    }
    lock.current = true
    setSaving(true)
    try {
      const body: SubscriptionTemplateBody = { name: name.trim(), enabled, include_all: includeAll, rules }
      if (template) await updateSubscriptionTemplate(template.id, body)
      else await createSubscriptionTemplate(body)
      message.success('已保存模板')
      onSaved()
      onClose()
    } catch (e) {
      message.error(errMsg(e))
    } finally {
      lock.current = false
      setSaving(false)
    }
  }

  return (
    <Modal title={template ? '编辑模板' : '新增模板'} width={MODAL_MD} open onCancel={onClose} onOk={submit} confirmLoading={saving}>
      <div className="um-group">
        <label>名称</label>
        <Input value={name} maxLength={128} placeholder="例如：日本节点" onChange={(e) => setName(e.target.value)} />
      </div>
      <div className="um-group" style={{ display: 'flex', gap: 24, alignItems: 'center', marginTop: 12 }}>
        <span><Switch checked={enabled} onChange={setEnabled} /> 启用</span>
        <span><Switch checked={includeAll} onChange={setIncludeAll} /> 包含全部节点（新节点自动加入）</span>
      </div>
      <div className="um-group" style={{ marginTop: 12 }}>
        <label>规则{includeAll ? '（仅排除规则生效）' : '（命中任一“包含”规则即选中）'}</label>
        {rules.length === 0 && <div className="faint" style={{ padding: '8px 0' }}>暂无规则</div>}
        {rules.map((r, i) => (
          <div key={i} style={{ display: 'flex', gap: 8, alignItems: 'center', marginTop: 8, flexWrap: 'wrap' }}>
            <Select
              style={{ width: 84 }}
              value={r.exclude ? 'exclude' : 'include'}
              options={[{ value: 'include', label: '包含' }, { value: 'exclude', label: '排除' }]}
              onChange={(v) => setRule(i, { exclude: v === 'exclude' })}
            />
            <Select
              style={{ width: 200 }}
              value={r.field}
              options={TEMPLATE_FIELDS}
              onChange={(v) => setRule(i, { field: v })}
            />
            <Select
              style={{ width: 84 }}
              value={r.match}
              options={TEMPLATE_MATCHES}
              onChange={(v) => setRule(i, { match: v })}
            />
            <Input
              style={{ flex: 1, minWidth: 140 }}
              value={r.value}
              maxLength={256}
              placeholder={r.match === 'regex' ? '正则表达式' : '匹配值'}
              onChange={(e) => setRule(i, { value: e.target.value })}
            />
            <Button onClick={() => setRules((cur) => cur.filter((_, idx) => idx !== i))}>删除</Button>
          </div>
        ))}
        <div style={{ marginTop: 10 }}>
          <Button onClick={() => setRules((cur) => [...cur, emptyRule()])}>添加规则</Button>
        </div>
      </div>
    </Modal>
  )
}

// SubscriptionTemplates manages rule-based node selection. A user bound to a
// template receives their explicit nodes plus every node the template matches.
export default function SubscriptionTemplates() {
  const [rows, setRows] = useState<SubscriptionTemplate[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [editing, setEditing] = useState<SubscriptionTemplate | null | undefined>(undefined)
  const [loaded, setLoaded] = useState(false)
  const generation = useRef(0)

  const load = () => {
    const gen = ++generation.current
    setLoading(true)
    setError(null)
    listSubscriptionTemplates()
      .then((next) => {
        if (gen === generation.current) {
          setRows(next)
          setLoaded(true)
        }
      })
      .catch((e) => {
        if (gen === generation.current) setError(errMsg(e))
      })
      .finally(() => {
        if (gen === generation.current) setLoading(false)
      })
  }
  useEffect(() => {
    load()
    return () => {
      generation.current++
    }
  }, [])

  const remove = async (t: SubscriptionTemplate) => {
    await deleteSubscriptionTemplate(t.id)
    message.success('已删除模板')
    load()
  }

  return (
    <div className="um-page">
      <PageHeader
        title="订阅模板"
        actions={<Button type="primary" onClick={() => setEditing(null)}>新增模板</Button>}
      />
      <RequestState loading={loading && !loaded} error={error} hasData={loaded} onRetry={load}>
        <Table<SubscriptionTemplate>
          size="small"
          rowKey="id"
          dataSource={rows}
          scroll={{ x: 640 }}
          locale={{ emptyText: '暂无模板。创建后在“用户”里绑定，新节点会自动进入订阅。' }}
          columns={[
            { title: '名称', dataIndex: 'name', width: 160, ellipsis: true },
            { title: '状态', dataIndex: 'enabled', width: 80, render: (v: boolean) => (v ? '启用' : <span className="faint">停用</span>) },
            { title: '规则', key: 'rules', render: (_: unknown, r) => describeTemplate(r) },
            { title: '用户', dataIndex: 'user_count', width: 70, align: 'right', render: (v?: number) => v ?? 0 },
            {
              title: '操作',
              key: 'actions',
              width: 140,
              render: (_: unknown, r) => (
                <span style={{ display: 'flex', gap: 8 }}>
                  <Button size="small" onClick={() => setEditing(r)}>编辑</Button>
                  <Popconfirm
                    title="删除模板？"
                    description={r.user_count ? `${r.user_count} 个用户正在使用，删除后将失去模板带来的节点。` : undefined}
                    onConfirm={() => remove(r)}
                  >
                    <Button size="small" danger>删除</Button>
                  </Popconfirm>
                </span>
              ),
            },
          ]}
        />
      </RequestState>
      {editing !== undefined && (
        <TemplateDialog template={editing ?? undefined} onClose={() => setEditing(undefined)} onSaved={load} />
      )}
    </div>
  )
}
