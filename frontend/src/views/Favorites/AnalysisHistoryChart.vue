<template>
  <el-card class="analysis-history-card" shadow="never">
    <template #header>
      <div class="card-header">
        <div class="header-left">
          <el-icon><TrendCharts /></el-icon>
          <span class="title">自选股分析历史趋势</span>
          <span class="subtitle" v-if="!loading">
            · {{ series.length }} 只股票 / {{ totalPoints }} 个分析点
          </span>
        </div>
        <div class="header-right">
          <el-select
            v-model="rangeDays"
            size="small"
            style="width: 110px; margin-right: 8px"
            @change="loadData"
          >
            <el-option label="近 1 月" :value="30" />
            <el-option label="近 3 月" :value="90" />
            <el-option label="近半年" :value="180" />
            <el-option label="近 1 年" :value="365" />
            <el-option label="全部" :value="0" />
          </el-select>
          <el-select
            v-model="selectedCodes"
            multiple
            filterable
            collapse-tags
            collapse-tags-tooltip
            placeholder="筛选股票"
            style="width: 280px; margin-right: 8px"
            @change="renderChart"
          >
            <el-option
              v-for="item in allOptions"
              :key="item.stock_code"
              :label="item.label"
              :value="item.stock_code"
            />
          </el-select>
          <el-button :icon="Refresh" size="small" @click="loadData" :loading="loading">
            刷新
          </el-button>
        </div>
      </div>
    </template>

    <div v-if="loading" class="loading-wrapper" v-loading="true" style="height: 400px"></div>
    <div v-else-if="series.length === 0" class="empty-wrapper">
      <el-empty description="暂无分析历史数据 —— 等待下一次定时分析，或在单股分析页手动发起一次" />
    </div>
    <div v-else>
      <div ref="chartRef" class="chart-container"></div>
      <div class="legend-note">
        <span class="legend-dot buy"></span> 买入
        <span class="legend-dot sell"></span> 卖出
        <span class="legend-dot hold"></span> 持有
        <span class="tip">· 实线 = 当时股价 · 虚线 = AI 目标价 · 悬停查看预计收益率 · 点击点位在新标签页打开完整分析报告</span>
      </div>
    </div>
  </el-card>
</template>

<script setup lang="ts">
import { ref, onMounted, onBeforeUnmount, computed, watch, nextTick } from 'vue'
import { ElMessage } from 'element-plus'
import { TrendCharts, Refresh } from '@element-plus/icons-vue'
import * as echarts from 'echarts'
import { useRouter } from 'vue-router'
import { favoritesApi } from '@/api/favorites'

const router = useRouter()

// 类型
interface AnalysisPoint {
  analysis_id: string
  analyzed_at: string
  current_price: number | null
  target_price: number | null
  expected_return: number | null
  action: string | null
  confidence: number | null
}
interface SeriesItem {
  stock_code: string
  stock_name: string
  market: string
  points: AnalysisPoint[]
}

// Props: 从父组件（Favorites 列表）接收当前 favorites，用于默认填充筛选项
// 图表自己调 API 拿历史数据。类型故意宽松（FavoriteItem 多字段可选），
// 我们只关心 stock_code，其它字段存在与否都无所谓。
interface Props {
  favorites?: Array<{ stock_code?: string; stock_name?: string; market?: string }>
}
const props = defineProps<Props>()

const loading = ref(false)
const series = ref<SeriesItem[]>([])
const selectedCodes = ref<string[]>([])
// 时间窗（天数）；0 = 全部。默认近半年
const rangeDays = ref<number>(180)
const chartRef = ref<HTMLElement | null>(null)
let chartInstance: echarts.ECharts | null = null

const allOptions = computed(() =>
  series.value.map(s => ({
    stock_code: s.stock_code,
    label: `${s.stock_name} (${s.stock_code}) · ${s.market}`,
  }))
)

const totalPoints = computed(() =>
  series.value.reduce((sum, s) => sum + s.points.length, 0)
)

// ---- action → 颜色 ----
const actionColor = (action: string | null): string => {
  if (!action) return '#909399'
  if (action.includes('买') || /buy/i.test(action)) return '#67C23A'
  if (action.includes('卖') || /sell/i.test(action)) return '#F56C6C'
  return '#909399'
}

const loadData = async () => {
  loading.value = true
  try {
    // rangeDays=0 表示"全部"，不传 days；limit 给一个大上限兜底
    const params: { limit: number; days?: number } = { limit: 2000 }
    if (rangeDays.value && rangeDays.value > 0) {
      params.days = rangeDays.value
    }
    const res: any = await favoritesApi.getAnalysisHistory(params)
    const data = res?.data ?? res
    series.value = data?.series ?? []
    // 默认只选"最近一次分析"最新的那一只股票，避免多条曲线同屏挤在一起
    // 时间取该 series 里最后一个点的 analyzed_at（points 已按时间正序）
    const pickLatestCode = (): string | null => {
      let bestCode: string | null = null
      let bestTs = -Infinity
      for (const s of series.value) {
        const last = s.points[s.points.length - 1]
        if (!last || !last.analyzed_at) continue
        const ts = new Date(last.analyzed_at).getTime()
        if (!Number.isNaN(ts) && ts > bestTs) {
          bestTs = ts
          bestCode = s.stock_code
        }
      }
      // 兜底：如果没有任何带时间的点，就选第一只
      return bestCode ?? (series.value[0]?.stock_code ?? null)
    }
    const latest = pickLatestCode()
    selectedCodes.value = latest ? [latest] : []
    await nextTick()
    renderChart()
  } catch (e: any) {
    console.error('加载分析历史失败:', e)
    ElMessage.error(e?.message || '加载分析历史失败')
  } finally {
    loading.value = false
  }
}

const renderChart = () => {
  if (!chartRef.value) return
  if (!chartInstance) {
    chartInstance = echarts.init(chartRef.value)
    // 点击点位 → 在新标签页打开对应的分析报告详情页
    chartInstance.on('click', (params: any) => {
      const p: AnalysisPoint | undefined = params?.data?.pointMeta
      if (p?.analysis_id) {
        const href = router.resolve({
          name: 'ReportDetail',
          params: { id: p.analysis_id },
        }).href
        window.open(href, '_blank', 'noopener')
      }
    })
  }

  const selected = new Set(selectedCodes.value)
  const visible = series.value.filter(s => selected.has(s.stock_code))

  // 生成 series: 每只股票两条线 —— 当时股价(实线)、目标价(虚线)
  const chartSeries: any[] = []
  const legend: string[] = []

  visible.forEach(s => {
    const nameReal = `${s.stock_name} · 当时价`
    const nameTarget = `${s.stock_name} · 目标价`
    legend.push(nameReal, nameTarget)

    chartSeries.push({
      name: nameReal,
      type: 'line',
      showSymbol: true,
      symbolSize: 10,
      smooth: false,
      data: s.points.map(p => ({
        value: [p.analyzed_at, p.current_price],
        itemStyle: { color: actionColor(p.action) },
        // 把点位元数据塞进去供 tooltip 用
        pointMeta: p,
        stockName: s.stock_name,
        seriesKind: 'current',
      })),
      connectNulls: true,
    })
    chartSeries.push({
      name: nameTarget,
      type: 'line',
      showSymbol: true,
      symbolSize: 8,
      smooth: false,
      lineStyle: { type: 'dashed', width: 1 },
      itemStyle: { opacity: 0.6 },
      data: s.points.map(p => ({
        value: [p.analyzed_at, p.target_price],
        pointMeta: p,
        stockName: s.stock_name,
        seriesKind: 'target',
      })),
      connectNulls: true,
    })
  })

  chartInstance.setOption(
    {
      tooltip: {
        trigger: 'item',
        formatter: (param: any) => {
          const d = param.data || {}
          const p: AnalysisPoint | undefined = d.pointMeta
          const kind = d.seriesKind === 'target' ? 'AI 目标价' : '当时股价'
          if (!p) return ''
          const t = new Date(p.analyzed_at).toLocaleString('zh-CN', { hour12: false })
          const rowsParts: string[] = [
            `<b>${d.stockName ?? ''}</b> (${p.action ?? '-'}) — ${kind}`,
            `时间: ${t}`,
            `当时价: ${p.current_price ?? '-'}`,
            `目标价: ${p.target_price ?? '-'}`,
          ]
          if (p.expected_return !== null && p.expected_return !== undefined) {
            rowsParts.push(`预计收益率: <b style="color:${p.expected_return >= 0 ? '#67C23A' : '#F56C6C'}">${p.expected_return >= 0 ? '+' : ''}${p.expected_return.toFixed(2)}%</b>`)
          }
          if (p.confidence !== null && p.confidence !== undefined) {
            const cp = p.confidence <= 1 ? (p.confidence * 100).toFixed(1) : p.confidence.toFixed(1)
            rowsParts.push(`置信度: ${cp}%`)
          }
          return rowsParts.join('<br/>')
        },
      },
      legend: {
        type: 'scroll',
        data: legend,
        bottom: 0,
      },
      grid: { top: 20, left: 48, right: 20, bottom: 60 },
      xAxis: {
        type: 'time',
        boundaryGap: false,
      },
      yAxis: {
        type: 'value',
        scale: true,
        splitLine: { lineStyle: { type: 'dashed' } },
      },
      series: chartSeries,
    },
    true,
  )
}

const handleResize = () => chartInstance?.resize()

onMounted(async () => {
  await loadData()
  window.addEventListener('resize', handleResize)
})

onBeforeUnmount(() => {
  window.removeEventListener('resize', handleResize)
  chartInstance?.dispose()
  chartInstance = null
})

// 外部 favorites 列表变化时（用户加/删自选股），刷新一下
watch(
  () => (props.favorites ?? []).map(f => f.stock_code).join(','),
  () => {
    loadData()
  },
)

/**
 * 外部调用：在图表中只显示某只股票，并把视口滚动到图表。
 * 如果当前 series 里还没有这只股票（刚加的自选股 + 还没有历史），
 * 先强制刷新一次，拿到后再切。
 */
const focusStock = async (stockCode: string) => {
  if (!stockCode) return
  const has = series.value.some(s => s.stock_code === stockCode)
  if (!has) {
    // 数据里还没这只，就先拉一次看能不能拿到
    await loadData()
  }
  selectedCodes.value = [stockCode]
  await nextTick()
  renderChart()
  // 滚动到图表位置
  chartRef.value?.scrollIntoView({ behavior: 'smooth', block: 'start' })
}

defineExpose({ focusStock })
</script>

<style scoped>
.analysis-history-card {
  margin-top: 16px;
}
.card-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 8px;
}
.header-left {
  display: flex;
  align-items: center;
  gap: 6px;
}
.header-left .title {
  font-weight: 600;
}
.header-left .subtitle {
  font-size: 13px;
  color: var(--el-text-color-secondary);
}
.header-right {
  display: flex;
  align-items: center;
}
.chart-container {
  width: 100%;
  height: 420px;
}
.empty-wrapper,
.loading-wrapper {
  display: flex;
  align-items: center;
  justify-content: center;
}
.legend-note {
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
.legend-note .tip {
  margin-left: 12px;
}
</style>
