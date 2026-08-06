import { h, onMounted, watch, nextTick } from 'vue'
import DefaultTheme from 'vitepress/theme'
import { useRoute } from 'vitepress'
import FormatBadges from './components/FormatBadges.vue'
import IntegrationBadges from './components/IntegrationBadges.vue'
import HeroQuery from './components/HeroQuery.vue'
import ArchDiagram from './components/ArchDiagram.vue'
import FeaturesIntro from './components/FeaturesIntro.vue'
import GetStartedCta from './components/GetStartedCta.vue'
import QueryFlow from './components/QueryFlow.vue'
import HeroBackdrop from './components/HeroBackdrop.vue'
import SystemDiagram from './components/SystemDiagram.vue'
import LatestRedirect from './components/LatestRedirect.vue'
import NotFound from './components/NotFound.vue'
import PreReleaseNotice from './components/PreReleaseNotice.vue'
import './custom.css'

// Reveal sections as they scroll into view. No-JS safe (the hidden state is only
// ever added by JS), and below-the-fold-only (so above-the-fold content never
// flashes). Skipped under reduced-motion.
function setupScrollReveal() {
    if (typeof window === 'undefined') return
    const reduce = window.matchMedia &&
        window.matchMedia('(prefers-reduced-motion: reduce)').matches
    requestAnimationFrame(() => {
        const targets = document.querySelectorAll(
            '.home-hero-badges, .arch, .feat-intro, .VPFeature, .cta'
        )
        if (!targets.length || reduce || !('IntersectionObserver' in window)) return
        const io = new IntersectionObserver((entries, obs) => {
            entries.forEach((e) => {
                if (e.isIntersecting) {
                    e.target.classList.add('in-view')
                    obs.unobserve(e.target)
                }
            })
        }, { rootMargin: '0px 0px -8% 0px', threshold: 0.08 })
        let landIndex = 0
        targets.forEach((el) => {
            if (el.getBoundingClientRect().top > window.innerHeight * 0.88) {
                // below the fold -> reveal on scroll
                el.classList.add('reveal')
                io.observe(el)
            } else {
                // already on screen at landing -> entrance, cascading after the hero
                el.style.animationDelay = (0.45 + landIndex * 0.12) + 's'
                el.classList.add('land-in')
                landIndex++
            }
        })
    })
}

// Fade content blocks up as they scroll into view on documentation pages.
// Below-the-fold-only (above-the-fold never hides -> no flash), reduced-motion
// safe, and re-run on every route change since VitePress swaps content in place.
function setupDocReveal() {
    if (typeof window === 'undefined') return
    const reduce = window.matchMedia &&
        window.matchMedia('(prefers-reduced-motion: reduce)').matches
    if (reduce || !('IntersectionObserver' in window)) return
    const doc = document.querySelector('.VPDoc .vp-doc')
    if (!doc) return
    const blocks = doc.querySelectorAll(
        ':scope > h2, :scope > h3, :scope > p, :scope > ul, :scope > ol, ' +
        ':scope > table, :scope > blockquote, :scope > .custom-block, ' +
        ':scope > div[class*="language-"], :scope > .vp-code-group'
    )
    const io = new IntersectionObserver((entries, obs) => {
        entries.forEach((e) => {
            if (e.isIntersecting) {
                e.target.classList.add('doc-in')
                obs.unobserve(e.target)
            }
        })
    }, { rootMargin: '0px 0px -6% 0px', threshold: 0.05 })
    blocks.forEach((el) => {
        if (el.getBoundingClientRect().top > window.innerHeight * 0.9) {
            el.classList.add('doc-reveal')
            io.observe(el)
        }
    })
}

const Layout = {
    setup() {
        const route = useRoute()
        onMounted(() => {
            setupScrollReveal()
            setupDocReveal()
        })
        // VitePress swaps page content without remounting the layout, so re-arm
        // the doc reveal after each navigation once the new DOM has painted.
        watch(() => route.path, () => {
            nextTick(() => requestAnimationFrame(setupDocReveal))
        })
        return () => h(DefaultTheme.Layout, null, {
            'home-hero-before': () => h(HeroBackdrop),
            'home-hero-image': () => h(HeroQuery),
            'home-hero-after': () =>
                h('div', { class: 'home-hero-badges' }, [
                    h(FormatBadges),
                    h(IntegrationBadges),
                    h(ArchDiagram)
                ]),
            'home-features-before': () => h(FeaturesIntro),
            'home-features-after': () => h(GetStartedCta),
            // Banner above the content on pre-release docs pages; the component
            // renders nothing on every other version.
            'doc-before': () => h(PreReleaseNotice),
            // Doubles as the `/docs/latest/...` catch-all: GitHub Pages serves
            // 404.html for unknown paths, so this rewrites the alias client-side
            // before falling back to a normal 404.
            'not-found': () => h(NotFound)
        })
    }
}

export default {
    extends: DefaultTheme,

    enhanceApp({ app }) {
        app.component('FormatBadges', FormatBadges)
        app.component('IntegrationBadges', IntegrationBadges)
        app.component('HeroQuery', HeroQuery)
        app.component('ArchDiagram', ArchDiagram)
        app.component('FeaturesIntro', FeaturesIntro)
        app.component('GetStartedCta', GetStartedCta)
        app.component('QueryFlow', QueryFlow)
        app.component('HeroBackdrop', HeroBackdrop)
        app.component('SystemDiagram', SystemDiagram)
        app.component('LatestRedirect', LatestRedirect)
    },

    Layout
}
