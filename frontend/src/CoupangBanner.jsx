import { useEffect, useMemo, useState } from 'react'
import { fetchCoupangAdLinks } from './api'

const MAX_LINKS = 4
const DEFAULT_DISCLOSURE = '이 포스팅은 쿠팡파트너스 활동의 일환으로, 이에 따른 일정액의 수수료를 제공받을 수 있습니다.'

const normalizeItems = (payload) => {
  const rawItems = Array.isArray(payload?.items) ? payload.items : []
  const dedup = new Set()
  const normalized = []

  rawItems.forEach((item, index) => {
    const link = String(item?.link || '').trim()
    if (!link || dedup.has(link)) return
    dedup.add(link)
    normalized.push({
      title: String(item?.title || `쿠팡 추천 ${index + 1}`).trim(),
      link,
      image: String(item?.image || '').trim(),
      meta: String(item?.meta || '').trim(),
      cta: String(item?.cta || '바로가기').trim(),
    })
  })

  return normalized.slice(0, MAX_LINKS)
}

export default function CoupangBanner() {
  const [items, setItems] = useState([])
  const [disclosure, setDisclosure] = useState(DEFAULT_DISCLOSURE)

  useEffect(() => {
    let mounted = true
    fetchCoupangAdLinks({ limit: MAX_LINKS })
      .then((payload) => {
        if (!mounted) return
        setItems(normalizeItems(payload))
        const text = String(payload?.disclosure || '').trim()
        if (text) setDisclosure(text)
      })
      .catch(() => {
        if (!mounted) return
        setItems([])
      })
    return () => {
      mounted = false
    }
  }, [])

  const featured = useMemo(() => items[0] || null, [items])
  const alternatives = useMemo(() => items.slice(1), [items])

  if (!featured) return null

  return (
    <div className="cp-inline" role="complementary" aria-label="쿠팡 제휴 광고">
      <div className="cp-inline-head">
        <span className="cp-inline-badge">AD · COUPANG PARTNERS</span>
        <span className="cp-inline-disclosure">{disclosure}</span>
      </div>
      <div className="cp-inline-title">메인 결과/분석 카드 추천 링크</div>
      <a
        className="cp-inline-cta"
        href={featured.link}
        target="_blank"
        rel="noopener noreferrer"
      >
        {featured.image ? (
          <img className="cp-inline-cta-thumb" src={featured.image} alt={featured.title || '쿠팡 추천'} loading="lazy" />
        ) : (
          <span className="cp-inline-cta-thumb cp-inline-cta-thumb-fallback">COUPANG</span>
        )}
        <span className="cp-inline-cta-copy">
          <span className="cp-inline-cta-title">{featured.title}</span>
          {featured.meta ? <span className="cp-inline-cta-meta">{featured.meta}</span> : null}
        </span>
        <strong>{featured.cta}</strong>
      </a>
      {alternatives.length ? (
        <div className="cp-inline-links">
          {alternatives.map((item) => (
            <a
              key={item.link}
              className="cp-inline-link"
              href={item.link}
              target="_blank"
              rel="noopener noreferrer"
            >
              {item.title}
            </a>
          ))}
        </div>
      ) : null}
    </div>
  )
}
