import { defineConfig } from 'vitepress'

// https://vitepress.dev/reference/site-config
export default defineConfig({
  markdown: {
    theme: {
      dark: 'dark-plus',
      light: 'light-plus',
    }
  },
  base: '/beacon/',
  ignoreDeadLinks: true,
  title: "Documentation",
  description: "Beacon Documentation",
  lastUpdated: true,
  themeConfig: {
    // https://vitepress.dev/reference/default-theme-config
    nav: [
      { text: 'Beacon', link: 'https://beacon.maris.nl/', target: '_blank', rel: 'noopener' },
      {
        text: 'How to install', items: [
          {
            text: '1.0.0 (latest)',
            link: '/docs/1.0.0-install/',
            activeMatch: '/docs/1.0.0-install/'
          },
        ]
      },
      {
        text: 'Query docs', items: [
          {
            text: '1.0.0 (latest)',
            link: '/docs/1.0.0/query-docs/',
            activeMatch: '/docs/1.0.0/query-docs/'
          }
        ]
      },
      {
        text: 'Available nodes',
        link: '/available-nodes/available-nodes',
        activeMatch: '/available-nodes/available-nodes'
      }
    ],

    sidebar: {
      '/available-nodes/': [
        {
          text: 'Euro-Argo',
          link: '/available-nodes/available-nodes#euro-argo',
        },
        {
          text: 'World Ocean Database',
          link: '/available-nodes/available-nodes#world-ocean-database',
        },
        {
          text: 'CMEMS CORA',
          link: '/available-nodes/available-nodes#cora-profiles-time-series',
        },
        {
          text: 'Access token',
          link: '/available-nodes/available-nodes#obtain-personal-access-token',
        }
      ],
      '/docs/1.0.0-install': [
        {
          text: 'Docs 1.0.0',
          items: [
            {
              text: 'Installation',
              link: '/docs/1.0.0-install/',
              items: [
                {
                  text: 'Docker',
                  link: '/docs/1.0.0-install#docker'
                },
                {
                  text: 'Activation',
                  link: '/docs/1.0.0-install#activation'
                },
                {
                  text: 'Troubleshooting',
                  link: '/docs/1.0.0-install#troubleshooting'
                },
              ]
            },
            {
              text: 'Configuration',
              link: '/docs/1.0.0-install/configuration'
            },
            {
              text: 'Importing Datasets',
              link: '/docs/1.0.0-install/importing-datasets/netcdf',
              items: [
                {
                  text: 'NetCDF',
                  link: '/docs/1.0.0-install/importing-datasets/netcdf',
                  // items: [
                  //   {
                  //     text: 'Default',
                  //     link: '/docs/1.0.0-install/importing-datasets/netcdf'
                  //   },
                  //   {
                  //     text: 'NetCDF Ragged',
                  //     link: '/docs/1.0.0-install/importing-datasets/ragged-netcdf'
                  //   },
                  //   {
                  //     text: 'NetCDF CMEMS CORA',
                  //     link: '/docs/1.0.0-install/importing-datasets/cora-netcdf'
                  //   },
                  //   {
                  //     text: 'NetCDF CMEMS BGC',
                  //     link: '/docs/1.0.0-install/importing-datasets/cora-netcdf'
                  //   }
                  // ]
                },
                {
                  text: 'CSV',
                  link: '/docs/1.0.0-install/importing-datasets/csv'
                },
                {
                  text: 'ODV',
                  link: '/docs/1.0.0-install/importing-datasets/odv'
                },
                {
                  text: 'Parquet',
                  link: '/docs/1.0.0-install/importing-datasets/parquet'
                }
              ]
            },
            {
              text: 'Releases',
              link: '/docs/1.0.0-install/releases'
            },
            {
              text: 'Hardware Recommendations',
              link: '/docs/1.0.0-install/recommendations/hardware'
            }]
        }
      ],
      '/docs/1.0.0/query-docs': [
        {
          text: 'Docs 1.0.0',
          items: [
            {
              text: 'Introduction',
              link: '/docs/1.0.0/query-docs'
            },
            {
              text: 'API request',
              link: '/docs/1.0.0/query-docs#sending-an-api-request'
            },
            {
              text: 'API query body',
              link: '/docs/1.0.0/query-docs#constructing-the-api-query-body',
              items: [
                {
                  text: 'Query parameters',
                  link: '/docs/1.0.0/query-docs#query-parameters'
                },
                {
                  text: 'Filters',
                  link: '/docs/1.0.0/query-docs#filters'
                },
                {
                  text: 'Distinct',
                  link: '/docs/1.0.0/query-docs.html#distinct'
                },
                {
                  text: 'Output formats',
                  link: '/docs/1.0.0/query-docs#output-format'
                }
              ]
            },
            {
              text: 'Union queries',
              link: '/docs/1.0.0/query-docs#union-queries'
            }
          ]
        }
      ]
    },

    socialLinks: [
      { icon: 'slack', link: 'https://join.slack.com/t/beacontechnic-wwa5548/shared_invite/zt-2dp1vv56r-tj_KFac0sAKNuAgUKPPDRg' },
    ]
  }
})
