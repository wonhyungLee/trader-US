import { useEffect, useMemo, useRef, useState } from 'react'
import { fetchCoupangBanner } from './api'

// 6h cooldown. Stored on "OPEN" (not on click) to avoid any "click reward" design.
const STORAGE_KEY_NEXT_AT = 'cp_auto_promo_next_at_v1'
const COOLDOWN_MS = 6 * 60 * 60 * 1000

const DEFAULT_THEME = {
  title: '쿠팡 추천 링크',
  tagline: '쿠팡광고2026/쿠팡광고링크.txt + GIF 기반 광고',
  cta: '지금 확인하기',
}

const PROMO_LINKS = [
  'https://link.coupang.com/a/dPJvzF',
  'https://link.coupang.com/a/dPJzZu',
  'https://link.coupang.com/a/dPJC4g',
  'https://link.coupang.com/a/dPJQFz',
  'https://link.coupang.com/a/dPJVxr',
  'https://link.coupang.com/a/dPJ2jt',
  'https://link.coupang.com/a/dPKcZs',
  'https://link.coupang.com/a/dPKgU0',
  'https://link.coupang.com/a/dPKjlp',
  'https://link.coupang.com/a/dPKIZ9',
  'https://link.coupang.com/a/dPKoN6',
  'https://link.coupang.com/a/dPKr4O',
  'https://link.coupang.com/a/dPKvE3',
  'https://link.coupang.com/a/dPKzjf',
  'https://link.coupang.com/a/dPKFV8',
  'https://link.coupang.com/a/dPKI7T',
]

const clampInt = (value, fallback) => {
  const num = Number.parseInt(String(value ?? ''), 10)
  return Number.isFinite(num) ? num : fallback
}

const extractCampaignCode = (link) => {
  const text = String(link || '').trim()
  if (!text) return ''
  const match = text.match(/\/a\/([A-Za-z0-9]+)/)
  return match ? String(match[1] || '').trim() : ''
}

const withBasePath = (value) => {
  const src = String(value || '').trim()
  if (!src) return ''
  if (/^(https?:)?\/\//i.test(src) || src.startsWith('data:')) return src
  const base = String(import.meta.env.BASE_URL || '/')
  const normalizedBase = base.endsWith('/') ? base : `${base}/`
  const normalizedSrc = src.replace(/^\/+/, '')
  return `${normalizedBase}${normalizedSrc}`
}

const readNextAt = () => {
  if (typeof localStorage === 'undefined') return 0
  return clampInt(localStorage.getItem(STORAGE_KEY_NEXT_AT), 0)
}

const writeNextAt = (ts) => {
  if (typeof localStorage === 'undefined') return
  localStorage.setItem(STORAGE_KEY_NEXT_AT, String(ts))
}

const normalizeItems = (payload) => {
  const rawItems = Array.isArray(payload?.items) ? payload.items : []
  const dedup = new Set()
  const normalized = []

  rawItems.forEach((item, index) => {
    const link = String(item?.link || '').trim()
    if (!link || dedup.has(link)) return
    dedup.add(link)

    const code = extractCampaignCode(link)
    const image = code ? `/coupang-gif/${code}_600.gif` : String(item?.image || '').trim()
    normalized.push({
      title: String(item?.title || `쿠팡 프로모션 ${index + 1}`).trim(),
      link,
      image,
      meta: String(item?.meta || '').trim(),
      cta: String(item?.cta || payload?.theme?.cta || DEFAULT_THEME.cta).trim(),
      badge: String(item?.badge || payload?.theme?.title || DEFAULT_THEME.title).trim(),
    })
  })

  return normalized
}

const pickRandom = (items) => {
  if (!Array.isArray(items) || !items.length) return null
  const idx = Math.floor(Math.random() * items.length)
  return items[idx] || null
}

const FALLBACK_ITEMS = PROMO_LINKS.map((link, index) => {
  const code = extractCampaignCode(link)
  return {
    title: `쿠팡 프로모션 ${index + 1}`,
    link,
    image: code ? `/coupang-gif/${code}_600.gif` : '',
    meta: '쿠팡 파트너스 추천 링크',
    cta: DEFAULT_THEME.cta,
    badge: DEFAULT_THEME.title,
  }
})

const isEligibleClick = (target) => {
  if (!target || !(target instanceof Element)) return false
  if (target.closest('.cp-pop')) return false

  const el = target.closest('button, [role="button"], a')
  if (!el) return false
  if (el.closest('[data-cp-ignore]')) return false

  return true
}

export default function CoupangAutoPopup({ disabled = false }) {
  const [open, setOpen] = useState(false)
  const [item, setItem] = useState(null)
  const [meta, setMeta] = useState(DEFAULT_THEME)
  const [loading, setLoading] = useState(false)
  const openRef = useRef(false)
  const loadingRef = useRef(false)

  useEffect(() => {
    openRef.current = open
  }, [open])

  const close = () => setOpen(false)

  const loadItem = async () => {
    if (loadingRef.current) return
    loadingRef.current = true
    setLoading(true)
    try {
      const payload = await fetchCoupangBanner({ limit: 12 })
      const items = normalizeItems(payload)
      const selected = pickRandom(items) || pickRandom(FALLBACK_ITEMS)
      setMeta(payload?.theme && typeof payload.theme === 'object' ? payload.theme : DEFAULT_THEME)
      setItem(selected || null)
    } catch {
      setMeta(DEFAULT_THEME)
      setItem(pickRandom(FALLBACK_ITEMS))
    } finally {
      loadingRef.current = false
      setLoading(false)
    }
  }

  const openIfAllowed = async () => {
    if (disabled) return
    if (openRef.current) return

    const now = Date.now()
    const nextAt = readNextAt()
    if (nextAt && now < nextAt) return

    writeNextAt(now + COOLDOWN_MS)
    setOpen(true)
    await loadItem()
  }

  useEffect(() => {
    if (disabled) return

    const handler = (event) => {
      if (disabled) return
      if (openRef.current) return
      if (!isEligibleClick(event.target)) return
      setTimeout(() => {
        void openIfAllowed()
      }, 0)
    }

    document.addEventListener('click', handler, true)
    return () => document.removeEventListener('click', handler, true)
  }, [disabled])

  useEffect(() => {
    if (!open) return
    const prev = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    return () => {
      document.body.style.overflow = prev
    }
  }, [open])

  const title = String(item?.title || '').trim()
  const image = withBasePath(item?.image)
  const link = String(item?.link || '').trim()
  const badge = String(item?.badge || meta?.title || DEFAULT_THEME.title).trim()
  const cta = String(item?.cta || meta?.cta || DEFAULT_THEME.cta).trim()
  const metaLine = String(item?.meta || meta?.tagline || '').trim()
  const hasLink = Boolean(link)

  const modal = useMemo(() => {
    if (!open) return null

    return (
      <div className="cp-pop" role="dialog" aria-modal="true" aria-label="쿠팡 프로모션 (광고)">
        <div className="cp-pop-backdrop" onClick={close} role="presentation" aria-label="닫기" />
        <div className={`cp-pop-card ${loading ? 'loading' : ''}`} role="document" onClick={(e) => e.stopPropagation()}>
          <button type="button" className="cp-pop-close" onClick={close} aria-label="닫기" title="닫기">
            ×
          </button>

          <div className="cp-pop-head">
            <span className="cp-pop-badge">{badge} · AD</span>
            <span className="cp-pop-tagline">{String(meta?.tagline || '진행 중인 쿠팡 프로모션을 확인해 보세요').trim()}</span>
          </div>

          <div className="cp-pop-body">
            {image ? <img className="cp-pop-img" src={image} alt={title || '추천 상품'} loading="lazy" /> : null}
            <div className="cp-pop-copy">
              <div className="cp-pop-title">{title || '오늘의 쿠팡 이벤트'}</div>
              {metaLine ? <div className="cp-pop-meta">{metaLine}</div> : null}
              {hasLink ? (
                <a className="cp-pop-cta" href={link} target="_blank" rel="noopener noreferrer" onClick={close}>
                  {cta}
                </a>
              ) : (
                <div className="cp-pop-meta">프로모션 링크를 불러오지 못했습니다.</div>
              )}
              <div className="cp-pop-disclosure">쿠팡파트너스 활동으로 수수료를 제공받을 수 있습니다.</div>
            </div>
          </div>
        </div>
      </div>
    )
  }, [open, loading, badge, meta, image, title, metaLine, hasLink, link, cta])

  return modal
}
