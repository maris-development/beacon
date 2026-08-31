import { h, onMounted, watch, nextTick } from 'vue'
import DefaultTheme from 'vitepress/theme'
import { useRoute } from 'vitepress'
import QueryFlow from './components/QueryFlow.vue'
import SystemDiagram from './components/SystemDiagram.vue'
import LatestRedirect from './components/LatestRedirect.vue'
import NotFound from './components/NotFound.vue'
import PreReleaseNotice from './components/PreReleaseNotice.vue'
import './custom.css'

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
            setupDocReveal()
        })
        // VitePress swaps page content without remounting the layout, so re-arm
        // the doc reveal after each navigation once the new DOM has painted.
        watch(() => route.path, () => {
            nextTick(() => requestAnimationFrame(setupDocReveal))
        })
        return () => h(DefaultTheme.Layout, null, {
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
        app.component('QueryFlow', QueryFlow)
        app.component('SystemDiagram', SystemDiagram)
        app.component('LatestRedirect', LatestRedirect)
    },

    Layout
}
