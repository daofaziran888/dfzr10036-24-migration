<template>
  <section class="page" data-testid="commit-migration">
    <header class="page-head">
      <div>
        <h2>提交清单迁移</h2>
        <p class="page-desc">
          兼容 v1 单路径快照与 v2 提交文件清单：按快照记录路径恢复，逐文件比对并核对回执。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="cm-refresh" type="button" @click="reload">刷新状态</button>
        <button class="btn primary" data-testid="cm-recover" type="button" :disabled="busy" @click="runRecovery">
          提交失败恢复
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">写入端版本</span>
        <strong class="stat-value" data-testid="cm-writer-version">{{ state?.写入端版本 ?? '—' }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">启动次数</span>
        <strong class="stat-value">{{ state?.启动次数 ?? '—' }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">快照文件</span>
        <strong class="stat-value">{{ state?.快照文件数 ?? '—' }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">差异行</span>
        <strong class="stat-value">{{ state?.差异行数 ?? '—' }}</strong>
      </article>
    </div>

    <h3>快照与清单</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>快照</th><th>清单版本</th><th>清单范围</th><th>比对状态</th>
          <th>已比对/待比对</th><th>差异行合计</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in comparison?.快照 ?? []" :key="row.快照编号" :data-testid="`cm-snapshot-${row.快照别名}`">
          <td>{{ row.快照别名 }}（{{ row.快照编号 }}）</td>
          <td>{{ row.清单版本 }}</td>
          <td class="mono">{{ (row.数据文件清单 ?? []).join('、') }}</td>
          <td :data-status="row.比对状态">
            {{ row.比对状态 }}{{ row.比对完成 ? '（完成）' : '（未完成）' }}
          </td>
          <td>{{ row.已比对文件数 }} / {{ row.待比对文件数 }}</td>
          <td>{{ row.差异行合计 }}</td>
        </tr>
        <tr v-if="!(comparison?.快照 ?? []).length">
          <td colspan="6" class="empty-state">暂无快照</td>
        </tr>
      </tbody>
    </table>

    <h3>逐文件消费与差异</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>文件</th><th>快照</th><th>清单版本</th><th>源端证据</th><th>目标端证据</th>
          <th>消费次数</th><th>比对状态</th><th>差异行</th><th>首次差异时间</th><th>回执</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="file in flatFiles" :key="`${file.快照编号}-${file.文件路径}`" :data-testid="`cm-file-${file.文件名}`">
          <td class="mono">{{ file.文件名 }}</td>
          <td>{{ file.快照别名 }}</td>
          <td>{{ file.清单版本 }}</td>
          <td>{{ file.源端证据 }}</td>
          <td>{{ file.目标端证据 }}</td>
          <td>{{ file.消费次数 }}</td>
          <td :data-status="file.比对状态">{{ file.比对状态 }}</td>
          <td>{{ file.差异行数 }}</td>
          <td>{{ file.首次差异时间 ?? '—' }}</td>
          <td>{{ file.回执编号 ?? '—' }}</td>
        </tr>
      </tbody>
    </table>

    <h3>恢复窗口</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>窗口</th><th>快照</th><th>清单版本</th><th>实际处理</th>
          <th>回执跳过</th><th>缺证延后</th><th>比对状态</th><th>差异行数</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="run in runs" :key="run.id">
          <td>{{ run.窗口 }}</td>
          <td>{{ run.快照别名 }}</td>
          <td>{{ run.清单版本 }}</td>
          <td class="mono">{{ (run.实际处理范围 ?? []).join('、') || '—' }}</td>
          <td class="mono">{{ (run.跳过回执范围 ?? []).join('、') || '—' }}</td>
          <td class="mono">{{ (run.延后缺证范围 ?? []).join('、') || '—' }}</td>
          <td>{{ run.比对状态 }}</td>
          <td>{{ run.差异行数 }}</td>
        </tr>
        <tr v-if="!runs.length">
          <td colspan="8" class="empty-state">还没有恢复窗口，点击“提交失败恢复”开始</td>
        </tr>
      </tbody>
    </table>

    <h3>差异行</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>快照</th><th>清单版本</th><th>文件</th><th>业务键</th>
          <th>源端值</th><th>目标端值</th><th>差异类型</th><th>首次出现时间</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in comparison?.差异行 ?? []" :key="row.id" :data-testid="`cm-diff-${row.业务键}`">
          <td>{{ row.快照别名 }}（{{ row.快照编号 }}）</td>
          <td>{{ row.清单版本 }}</td>
          <td class="mono">{{ row.文件名 }}</td>
          <td>{{ row.业务键 }}</td>
          <td>{{ row.源端值 }}</td>
          <td>{{ row.目标端值 }}</td>
          <td>{{ row.差异类型 }}</td>
          <td>{{ row.首次出现时间 }}</td>
        </tr>
        <tr v-if="!(comparison?.差异行 ?? []).length">
          <td colspan="8" class="empty-state">暂无差异行</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span v-if="lastMessage" data-testid="cm-message">{{ lastMessage }}</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { fetchJson, request } from '@/api/client'

type CommitFile = {
  文件名: string
  文件路径: string
  快照编号: string
  快照别名: string
  清单版本: string
  源端证据: string
  目标端证据: string
  消费次数: number
  比对状态: string
  差异行数: number
  首次差异时间: string | null
  回执编号: string | null
}

type CommitSnapshot = {
  快照编号: string
  快照别名: string
  清单版本: string
  数据文件清单: string[]
  比对状态: string
  比对完成: boolean
  已比对文件数: number
  待比对文件数: number
  差异行合计: number
  文件: CommitFile[]
}

type Comparison = {
  快照: CommitSnapshot[]
  差异行: { id: number; 快照编号: string; 快照别名: string; 清单版本: string; 文件名: string; 业务键: string; 源端值: string; 目标端值: string; 差异类型: string; 首次出现时间: string }[]
}

type CommitState = {
  写入端版本: string
  启动次数: number
  快照文件数: number
  差异行数: number
}

const state = ref<CommitState | null>(null)
const comparison = ref<Comparison | null>(null)
type RecoveryRun = {
  id: number
  窗口: string
  快照别名: string
  清单版本: string
  实际处理范围: string[]
  跳过回执范围: string[]
  延后缺证范围: string[]
  比对状态: string
  差异行数: number
}

const runs = ref<RecoveryRun[]>([])
const busy = ref(false)
const lastMessage = ref('')
const errorMessage = ref('')

const flatFiles = computed(() =>
  (comparison.value?.快照 ?? []).flatMap((snapshot) => snapshot.文件 ?? []),
)

async function reload() {
  errorMessage.value = ''
  try {
    state.value = await fetchJson<CommitState>('/api/commit/state')
    comparison.value = await fetchJson<Comparison>('/api/commit/comparison')
    const payload = await fetchJson<{ items: RecoveryRun[] }>('/api/commit/recovery')
    runs.value = payload.items
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '迁移状态读取失败'
  }
}

async function runRecovery() {
  busy.value = true
  errorMessage.value = ''
  lastMessage.value = ''
  try {
    const response = await request('/api/commit/recovery', { method: 'POST' })
    if (!response.ok) throw new Error(`接口返回 ${response.status}`)
    const payload = await response.json()
    lastMessage.value = `恢复窗口 ${payload.窗口}：处理 ${payload.处理快照数} 份快照`
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '提交失败恢复失败'
  } finally {
    busy.value = false
  }
}

onMounted(reload)
</script>
