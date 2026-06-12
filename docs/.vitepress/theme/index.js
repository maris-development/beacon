import { h } from 'vue'
import DefaultTheme from 'vitepress/theme'
import FormatBadges from './components/FormatBadges.vue'
import IntegrationBadges from './components/IntegrationBadges.vue'
import HeroQuery from './components/HeroQuery.vue'
import ArchDiagram from './components/ArchDiagram.vue'
import FeaturesIntro from './components/FeaturesIntro.vue'
import GetStartedCta from './components/GetStartedCta.vue'
import './custom.css'

export default {
    extends: DefaultTheme,

    enhanceApp({ app }) {
        app.component('FormatBadges', FormatBadges)
        app.component('IntegrationBadges', IntegrationBadges)
        app.component('HeroQuery', HeroQuery)
        app.component('ArchDiagram', ArchDiagram)
        app.component('FeaturesIntro', FeaturesIntro)
        app.component('GetStartedCta', GetStartedCta)
    },

    Layout() {
        return h(DefaultTheme.Layout, null, {
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