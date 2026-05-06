<template>
  <div class="reports">
    <!-- 页面标题 -->
    <div class="page-header">
      <h1 class="page-title">
        <el-icon><Document /></el-icon>
        分析报告
      </h1>
      <p class="page-description">
        每支股票最新的分析报告，点击股票名称查看历史趋势曲线
      </p>
    </div>

    <!-- 筛选和操作栏 -->
    <el-card class="filter-card" shadow="never">
      <el-row :gutter="16" align="middle">
        <el-col :span="8">
          <el-input
            v-model="searchKeyword"
            placeholder="搜索股票代码或名称"
            clearable
            @input="handleSearch"
          >
            <template #prefix>
              <el-icon><Search /></el-icon>
            </template>
          </el-input>
        </el-col>

        <el-col :span="4">
          <el-select v-model="marketFilter" placeholder="市场筛选" clearable @change="handleMarketChange">
            <el-option label="A股" value="A股" />
            <el-option label="港股" value="港股" />
            <el-option label="美股" value="美股" />
          </el-select>
        </el-col>

        <el-col :span="12">
          <div class="action-buttons">
            <el-button
              v-if="selectedReports.length > 0"
              type="warning"
              @click="showBatchAnalysisDialog"
            >
              <el-icon><DataAnalysis /></el-icon>
              批量分析 ({{ selectedReports.length }})
            </el-button>
            <el-button @click="refreshReports">
              <el-icon><Refresh /></el-icon>
              刷新
            </el-button>
          </div>
        </el-col>
      </el-row>
    </el-card>

    <!-- 报告列表 -->
    <el-card class="reports-list-card" shadow="never">
      <el-table
        :data="reports"
        v-loading="loading"
        style="width: 100%"
        @selection-change="handleSelectionChange"
      >
        <el-table-column type="selection" width="45" />
        <el-table-column prop="stock_code" label="股票代码" width="120" sortable>
          <template #default="{ row }">
            <span class="stock-code">{{ row.stock_code }}</span>
          </template>
        </el-table-column>

        <el-table-column prop="stock_name" label="股票名称" min-width="140" sortable>
          <template #default="{ row }">
            <el-link type="primary" @click="openHistoryChart(row)" :underline="false">
              {{ row.stock_name }}
            </el-link>
          </template>
        </el-table-column>

        <el-table-column prop="market_type" label="市场" width="80" sortable>
          <template #default="{ row }">
            <el-tag size="small" :type="getMarketTagType(row.market_type)">
              {{ row.market_type }}
            </el-tag>
          </template>
        </el-table-column>

        <el-table-column prop="current_price" label="当前价格" width="100" align="right" sortable :sort-method="(a: any, b: any) => sortNum(a.current_price, b.current_price)">
          <template #default="{ row }">
            <span v-if="row.current_price != null">{{ formatPrice(row.current_price) }}</span>
            <span v-else class="text-gray">-</span>
          </template>
        </el-table-column>

        <el-table-column prop="change_percent" label="涨跌幅" width="100" align="right" sortable :sort-method="(a: any, b: any) => sortNum(a.change_percent, b.change_percent)">
          <template #default="{ row }">
            <span
              v-if="row.change_percent != null"
              :style="{ color: row.change_percent > 0 ? '#F56C6C' : row.change_percent < 0 ? '#67C23A' : '' }"
            >
              {{ row.change_percent > 0 ? '+' : '' }}{{ row.change_percent.toFixed(2) }}%
            </span>
            <span v-else class="text-gray">-</span>
          </template>
        </el-table-column>

        <el-table-column prop="target_price" label="AI目标价" width="100" align="right" sortable :sort-method="(a: any, b: any) => sortNum(a.target_price, b.target_price)">
          <template #default="{ row }">
            <span v-if="row.target_price != null">{{ formatPrice(row.target_price) }}</span>
            <span v-else class="text-gray">-</span>
          </template>
        </el-table-column>

        <el-table-column prop="expected_return" label="预计收益率" width="110" align="right" sortable :sort-method="(a: any, b: any) => sortNum(a.expected_return, b.expected_return)">
          <template #default="{ row }">
            <span
              v-if="row.expected_return != null"
              :style="{ color: row.expected_return >= 0 ? '#67C23A' : '#F56C6C', fontWeight: 'bold' }"
            >
              {{ row.expected_return >= 0 ? '+' : '' }}{{ row.expected_return.toFixed(2) }}%
            </span>
            <span v-else class="text-gray">-</span>
          </template>
        </el-table-column>

        <el-table-column prop="action" label="AI建议" width="100" sortable>
          <template #default="{ row }">
            <el-tag v-if="row.action" :type="getActionTagType(row.action)" size="small">
              {{ row.action }}
            </el-tag>
            <span v-else class="text-gray">-</span>
          </template>
        </el-table-column>

        <el-table-column prop="model_info" label="分析模型" width="180">
          <template #default="{ row }">
            <el-tag v-if="row.model_info && row.model_info !== 'Unknown'" type="info" size="small">
              {{ row.model_info }}
            </el-tag>
            <span v-else class="text-gray">-</span>
          </template>
        </el-table-column>

        <el-table-column prop="created_at" label="最新分析时间" width="180" sortable>
          <template #default="{ row }">
            {{ formatTime(row.created_at) }}
          </template>
        </el-table-column>

        <el-table-column label="操作" width="240" fixed="right">
          <template #default="{ row }">
            <el-button type="text" size="small" @click="analyzeStock(row)">
              分析
            </el-button>
            <el-button type="text" size="small" @click="viewReport(row)">
              查看
            </el-button>
            <el-dropdown
              trigger="click"
              @command="(format: string) => downloadReport(row, format)"
            >
              <el-button type="text" size="small">
                下载 <el-icon class="el-icon--right"><arrow-down /></el-icon>
              </el-button>
              <template #dropdown>
                <el-dropdown-menu>
                  <el-dropdown-item command="markdown">Markdown</el-dropdown-item>
                  <el-dropdown-item command="docx">Word</el-dropdown-item>
                  <el-dropdown-item command="pdf">PDF</el-dropdown-item>
                  <el-dropdown-item command="json" divided>JSON</el-dropdown-item>
                </el-dropdown-menu>
              </template>
            </el-dropdown>
            <el-button
              type="text"
              size="small"
              @click="deleteReport(row)"
              style="color: var(--el-color-danger)"
            >
              删除
            </el-button>
          </template>
        </el-table-column>
      </el-table>

      <!-- 分页 -->
      <div class="pagination-wrapper">
        <el-pagination
          v-model:current-page="currentPage"
          v-model:page-size="pageSize"
          :page-sizes="[20, 50, 100]"
          :total="totalReports"
          layout="total, sizes, prev, pager, next, jumper"
          @size-change="handleSizeChange"
          @current-change="handleCurrentChange"
        />
      </div>
    </el-card>

    <!-- 历史曲线图 Dialog -->
    <el-dialog
      v-model="chartDialogVisible"
      :title="`${chartStock.stock_name}（${chartStock.stock_code}）分析历史趋势`"
      width="900px"
      destroy-on-close
    >
      <div v-if="chartLoading" v-loading="true" style="height: 420px"></div>
      <div v-else-if="chartPoints.length === 0" style="height: 200px; display:flex; align-items:center; justify-content:center;">
        <el-empty description="暂无该股票的历史分析数据" />
      </div>
      <div v-else>
        <div ref="chartRef" class="history-chart"></div>
        <div class="chart-legend-note">
          <span class="legend-dot buy"></span> 买入
          <span class="legend-dot sell"></span> 卖出
          <span class="legend-dot hold"></span> 持有
          <span class="tip">· 实线 = 当时股价 · 虚线 = AI 目标价 · 悬停查看预计收益率</span>
        </div>
      </div>
    </el-dialog>

    <!-- 批量分析 Dialog -->
    <el-dialog
      v-model="batchDialogVisible"
      title="批量 AI 分析"
      width="520px"
    >
      <el-alert type="info" :closable="false" style="margin-bottom: 16px;">
        已选择 <strong>{{ selectedReports.length }}</strong> 只股票，将逐只发起 AI 分析任务
      </el-alert>

      <el-form label-width="100px">
        <el-form-item label="分析深度">
          <el-radio-group v-model="batchDepth">
            <div style="margin-bottom: 4px"><el-radio label="快速">1级 · 快速 (2-4 分钟)</el-radio></div>
            <div style="margin-bottom: 4px"><el-radio label="基础">2级 · 基础 (4-6 分钟)</el-radio></div>
            <div style="margin-bottom: 4px"><el-radio label="标准">3级 · 标准 (6-10 分钟，推荐)</el-radio></div>
            <div style="margin-bottom: 4px"><el-radio label="深度">4级 · 深度 (10-15 分钟)</el-radio></div>
            <div><el-radio label="全面">5级 · 全面 (15-25 分钟)</el-radio></div>
          </el-radio-group>
        </el-form-item>
      </el-form>

      <el-alert
        v-if="selectedReports.length > 10"
        type="warning"
        :closable="false"
        style="margin-top: 12px;"
      >
        后端单次最多 10 只，本次将分 {{ Math.ceil(selectedReports.length / 10) }} 批依次提交
      </el-alert>

      <template #footer>
        <el-button @click="batchDialogVisible = false">取消</el-button>
        <el-button type="primary" :loading="batchLoading" @click="handleBatchAnalysis">
          开始分析
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onBeforeUnmount, nextTick } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import {
  Document,
  Search,
  Refresh,
  ArrowDown,
  DataAnalysis
} from '@element-plus/icons-vue'
import { useAuthStore } from '@/stores/auth'
import { formatDateTime } from '@/utils/datetime'
import { normalizeMarketForAnalysis } from '@/utils/market'
import { analysisApi } from '@/api/analysis'
import * as echarts from 'echarts'

const router = useRouter()
const authStore = useAuthStore()

// ---- 列表相关 ----
const loading = ref(false)
const searchKeyword = ref('')
const marketFilter = ref('')
const currentPage = ref(1)
const pageSize = ref(20)
const totalReports = ref(0)
const reports = ref<any[]>([])
const selectedReports = ref<any[]>([])

const handleSelectionChange = (selection: any[]) => {
  selectedReports.value = selection
}

const fetchReports = async () => {
  loading.value = true
  try {
    const params = new URLSearchParams({
      page: currentPage.value.toString(),
      page_size: pageSize.value.toString()
    })
    if (searchKeyword.value) params.append('search_keyword', searchKeyword.value)
    if (marketFilter.value) params.append('market_filter', marketFilter.value)

    const response = await fetch(`/api/reports/latest-per-stock?${params}`, {
      headers: { 'Authorization': `Bearer ${authStore.token}` }
    })
    if (!response.ok) throw new Error(`HTTP ${response.status}`)

    const result = await response.json()
    if (result.success) {
      reports.value = result.data.reports
      totalReports.value = result.data.total
    } else {
      throw new Error(result.message || '获取报告列表失败')
    }
  } catch (error) {
    console.error('获取报告列表失败:', error)
    ElMessage.error('获取报告列表失败')
  } finally {
    loading.value = false
  }
}

const handleSearch = () => { currentPage.value = 1; fetchReports() }
const handleMarketChange = () => { currentPage.value = 1; fetchReports() }
const handleSizeChange = () => { currentPage.value = 1; fetchReports() }
const handleCurrentChange = () => { fetchReports() }
const refreshReports = () => { fetchReports() }

// ---- 批量分析 ----
const batchDialogVisible = ref(false)
const batchDepth = ref('标准')
const batchLoading = ref(false)

const showBatchAnalysisDialog = () => {
  if (selectedReports.value.length === 0) {
    ElMessage.warning('请先选择要分析的股票')
    return
  }
  batchDialogVisible.value = true
}

const handleBatchAnalysis = async () => {
  if (selectedReports.value.length === 0) return
  batchLoading.value = true
  try {
    const chunkSize = 10
    const stocks = selectedReports.value.slice()
    const chunks: typeof stocks[] = []
    for (let i = 0; i < stocks.length; i += chunkSize) {
      chunks.push(stocks.slice(i, i + chunkSize))
    }

    let successBatches = 0
    let totalTasks = 0

    for (let i = 0; i < chunks.length; i++) {
      const chunk = chunks[i]
      const symbols = chunk.map((s: any) => s.stock_code).filter(Boolean)
      const batchTitle = chunks.length > 1
        ? `报告页批量分析 ${i + 1}/${chunks.length}`
        : `报告页批量分析（${symbols.length} 只）`
      try {
        const res: any = await analysisApi.startBatchAnalysis({
          title: batchTitle,
          symbols,
          parameters: { market_type: 'auto', research_depth: batchDepth.value } as any,
        })
        if (res?.success) {
          successBatches++
          totalTasks += res.data?.total_tasks ?? symbols.length
        }
      } catch (e: any) {
        console.error(`批量分析第 ${i + 1} 批失败:`, e)
      }
    }

    if (successBatches > 0) {
      ElMessage.success(`已提交 ${totalTasks} 个分析任务，请到「任务中心」查看`)
    } else {
      ElMessage.error('批量分析提交失败')
    }
    batchDialogVisible.value = false
  } catch (e: any) {
    ElMessage.error(e?.message || '批量分析失败')
  } finally {
    batchLoading.value = false
  }
}

const viewReport = (row: any) => {
  router.push(`/reports/view/${row.id}`)
}

const analyzeStock = (row: any) => {
  const href = router.resolve({
    name: 'SingleAnalysis',
    query: { stock: row.stock_code, market: normalizeMarketForAnalysis(row.market_type || 'A股') }
  }).href
  window.open(href, '_blank', 'noopener')
}

const downloadReport = async (report: any, format: string = 'markdown') => {
  try {
    const loadingMsg = ElMessage({ message: `正在生成报告...`, type: 'info', duration: 0 })
    const response = await fetch(`/api/reports/${report.id}/download?format=${format}`, {
      headers: { 'Authorization': `Bearer ${authStore.token}` }
    })
    loadingMsg.close()
    if (!response.ok) throw new Error(await response.text() || `HTTP ${response.status}`)

    const blob = await response.blob()
    const url = window.URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    const extMap: Record<string, string> = { markdown: 'md', docx: 'docx', pdf: 'pdf', json: 'json' }
    a.download = `${report.stock_code}_分析报告.${extMap[format] || 'txt'}`
    document.body.appendChild(a)
    a.click()
    window.URL.revokeObjectURL(url)
    document.body.removeChild(a)
    ElMessage.success('下载成功')
  } catch (error: any) {
    ElMessage.error(`下载失败: ${error.message || '未知错误'}`)
  }
}

const deleteReport = async (report: any) => {
  try {
    await ElMessageBox.confirm(
      `确定要删除 ${report.stock_name}(${report.stock_code}) 的最新分析报告吗？`,
      '确认删除',
      { confirmButtonText: '确定', cancelButtonText: '取消', type: 'warning' }
    )
    const response = await fetch(`/api/reports/${report.id}`, {
      method: 'DELETE',
      headers: { 'Authorization': `Bearer ${authStore.token}` }
    })
    if (!response.ok) throw new Error(`HTTP ${response.status}`)
    const result = await response.json()
    if (result.success) {
      ElMessage.success('报告已删除')
      refreshReports()
    } else {
      throw new Error(result.message || '删除失败')
    }
  } catch (error: any) {
    if (error !== 'cancel' && error?.message !== 'cancel') {
      ElMessage.error('删除报告失败')
    }
  }
}

// ---- 辅助函数 ----
const formatTime = (time: string) => formatDateTime(time)

const formatPrice = (price: number) => {
  if (price >= 1000) return price.toFixed(0)
  if (price >= 100) return price.toFixed(1)
  return price.toFixed(2)
}

const sortNum = (a: any, b: any) => {
  const va = a ?? -Infinity
  const vb = b ?? -Infinity
  return va - vb
}

const getMarketTagType = (market: string): any => {
  if (market === 'A股') return 'primary'
  if (market === '港股') return 'warning'
  if (market === '美股') return 'danger'
  return 'info'
}

const getActionTagType = (action: string) => {
  if (action?.includes('买')) return 'success'
  if (action?.includes('卖')) return 'danger'
  return 'info'
}

// ---- 历史曲线图 Dialog ----
const chartDialogVisible = ref(false)
const chartLoading = ref(false)
const chartStock = ref<{ stock_code: string; stock_name: string }>({ stock_code: '', stock_name: '' })
const chartPoints = ref<any[]>([])
const chartRef = ref<HTMLElement | null>(null)
let chartInstance: echarts.ECharts | null = null

const actionColor = (action: string | null): string => {
  if (!action) return '#909399'
  if (action.includes('买') || /buy/i.test(action)) return '#67C23A'
  if (action.includes('卖') || /sell/i.test(action)) return '#F56C6C'
  return '#909399'
}

const openHistoryChart = async (row: any) => {
  chartStock.value = { stock_code: row.stock_code, stock_name: row.stock_name }
  chartDialogVisible.value = true
  chartLoading.value = true
  chartPoints.value = []

  try {
    const params = new URLSearchParams({ stock_code: row.stock_code, days: '365', limit: '200' })
    const response = await fetch(`/api/reports/stock-history?${params}`, {
      headers: { 'Authorization': `Bearer ${authStore.token}` }
    })
    if (!response.ok) throw new Error(`HTTP ${response.status}`)
    const result = await response.json()
    if (result.success) {
      const s = result.data.series?.[0]
      chartPoints.value = s?.points ?? []
    }
  } catch (e: any) {
    ElMessage.error('加载分析历史失败')
    console.error(e)
  } finally {
    chartLoading.value = false
  }

  await nextTick()
  renderChart()
}

const renderChart = () => {
  if (!chartRef.value || chartPoints.value.length === 0) return

  if (chartInstance) {
    chartInstance.dispose()
  }
  chartInstance = echarts.init(chartRef.value)

  // 点击点位 → 在新标签页打开对应的分析报告详情页
  chartInstance.on('click', (params: any) => {
    const p = params?.data?.pointMeta
    if (p?.analysis_id) {
      const href = router.resolve({
        name: 'ReportDetail',
        params: { id: p.analysis_id },
      }).href
      window.open(href, '_blank', 'noopener')
    }
  })

  const points = chartPoints.value
  const stockName = chartStock.value.stock_name

  const chartSeries: any[] = [
    {
      name: `${stockName} · 当时价`,
      type: 'line',
      showSymbol: true,
      symbolSize: 10,
      smooth: false,
      connectNulls: true,
      data: points.map((p: any) => ({
        value: [p.analyzed_at, p.current_price],
        itemStyle: { color: actionColor(p.action) },
        pointMeta: p,
      })),
    },
    {
      name: `${stockName} · 目标价`,
      type: 'line',
      showSymbol: true,
      symbolSize: 8,
      smooth: false,
      connectNulls: true,
      lineStyle: { type: 'dashed', width: 1 },
      itemStyle: { opacity: 0.6 },
      data: points.map((p: any) => ({
        value: [p.analyzed_at, p.target_price],
        pointMeta: p,
      })),
    },
  ]

  chartInstance.setOption({
    tooltip: {
      trigger: 'item',
      formatter: (param: any) => {
        const d = param.data || {}
        const p = d.pointMeta
        if (!p) return ''
        const t = new Date(p.analyzed_at).toLocaleString('zh-CN', { hour12: false })
        const rows = [
          `<b>${stockName}</b> (${p.action ?? '-'})`,
          `时间: ${t}`,
          `当时价: ${p.current_price ?? '-'}`,
          `目标价: ${p.target_price ?? '-'}`,
        ]
        if (p.expected_return != null) {
          const color = p.expected_return >= 0 ? '#67C23A' : '#F56C6C'
          const sign = p.expected_return >= 0 ? '+' : ''
          rows.push(`预计收益: <b style="color:${color}">${sign}${p.expected_return.toFixed(2)}%</b>`)
        }
        if (p.confidence != null) {
          const cp = p.confidence <= 1 ? (p.confidence * 100).toFixed(1) : p.confidence.toFixed(1)
          rows.push(`置信度: ${cp}%`)
        }
        rows.push(`<span style="color:#409EFF;font-size:11px">点击查看完整报告</span>`)
        return rows.join('<br/>')
      },
    },
    legend: {
      data: [`${stockName} · 当时价`, `${stockName} · 目标价`],
      bottom: 0,
    },
    grid: { top: 20, left: 60, right: 20, bottom: 50 },
    xAxis: { type: 'time', boundaryGap: false },
    yAxis: { type: 'value', scale: true, splitLine: { lineStyle: { type: 'dashed' } } },
    series: chartSeries,
  })
}

const handleChartResize = () => chartInstance?.resize()

onMounted(() => {
  fetchReports()
  window.addEventListener('resize', handleChartResize)
})

onBeforeUnmount(() => {
  window.removeEventListener('resize', handleChartResize)
  chartInstance?.dispose()
  chartInstance = null
})
</script>

<style lang="scss" scoped>
.reports {
  .page-header {
    margin-bottom: 24px;
    .page-title {
      display: flex;
      align-items: center;
      gap: 8px;
      font-size: 24px;
      font-weight: 600;
      color: var(--el-text-color-primary);
      margin: 0 0 8px 0;
    }
    .page-description {
      color: var(--el-text-color-regular);
      margin: 0;
    }
  }

  .filter-card {
    margin-bottom: 24px;
    .action-buttons {
      display: flex;
      gap: 8px;
      justify-content: flex-end;
    }
  }

  .reports-list-card {
    .stock-code {
      font-family: 'Monaco', 'Menlo', monospace;
      font-size: 13px;
    }
    .text-gray {
      color: var(--el-text-color-placeholder);
    }
    .pagination-wrapper {
      display: flex;
      justify-content: center;
      margin-top: 24px;
    }
  }
}

.history-chart {
  width: 100%;
  height: 420px;
}

.chart-legend-note {
  margin-top: 8px;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}
.legend-dot {
  display: inline-block;
  width: 10px;
  height: 10px;
  border-radius: 50%;
  margin-right: 4px;
  margin-left: 12px;
  vertical-align: middle;
}
.legend-dot.buy { background: #67C23A; }
.legend-dot.sell { background: #F56C6C; }
.legend-dot.hold { background: #909399; }
.chart-legend-note .tip {
  margin-left: 12px;
}
</style>
