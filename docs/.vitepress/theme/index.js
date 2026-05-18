import { h } from 'vue'
import DefaultTheme from 'vitepress/theme'
import FormatBadges from './components/FormatBadges.vue'
import IntegrationBadges from './components/IntegrationBadges.vue'
import './custom.css'

export default {
    extends: DefaultTheme,

    enhanceApp({ app }) {
        app.component('FormatBadges', FormatBadges)
        app.component('IntegrationBadges', IntegrationBadges)
    },

    Layout() {
        return h(DefaultTheme.Layout, null, {
            'home-hero-after': () =>
                h('div', { class: 'home-hero-badges' }, [
                    h(FormatBadges),
                    h(IntegrationBadges)
                ])
        })
    }
}