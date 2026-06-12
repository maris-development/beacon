import { h, onMounted } from 'vue'
import DefaultTheme from 'vitepress/theme'
import FormatBadges from './components/FormatBadges.vue'
import IntegrationBadges from './components/IntegrationBadges.vue'
import HeroQuery from './components/HeroQuery.vue'
import ArchDiagram from './components/ArchDiagram.vue'
import FeaturesIntro from './components/FeaturesIntro.vue'
import GetStartedCta from './components/GetStartedCta.vue'
import QueryFlow from './components/QueryFlow.vue'
import HeroBackdrop from './components/HeroBackdrop.vue'
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

const Layout = {
    setup() {
        onMounted(setupScrollReveal)
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
            'home-features-after': () => h(GetStartedCta)
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
    },

    Layout
}
