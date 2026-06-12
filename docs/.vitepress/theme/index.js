import { h } from 'vue'
import DefaultTheme from 'vitepress/theme'
import FormatBadges from './components/FormatBadges.vue'
import IntegrationBadges from './components/IntegrationBadges.vue'
import HeroQuery from './components/HeroQuery.vue'
import ArchDiagram from './components/ArchDiagram.vue'
import './custom.css'

export default {
    extends: DefaultTheme,

    enhanceApp({ app }) {
        app.component('FormatBadges', FormatBadges)
        app.component('IntegrationBadges', IntegrationBadges)
        app.component('HeroQuery', HeroQuery)
        app.component('ArchDiagram', ArchDiagram)
    },

    Layout() {
        return h(DefaultTheme.Layout, null, {
            'home-hero-image': () => h(HeroQuery),
            'home-hero-after': () =>
                h('div', { class: 'home-hero-badges' }, [
                    h(FormatBadges),
                    h(IntegrationBadges),
                    h(ArchDiagram)
                ])
        })
    }
}