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
            text: '1.0.1 (latest)',
            link: '/docs/1.0.1-install/',
            activeMatch: '/docs/1.0.1-install/'
          },
        ]
      },
      {
        text: 'Query docs', items: [
          {
            text: '1.0.1 (latest)',
            link: '/docs/1.0.1/query-docs/data-lake',
            activeMatch: '/docs/1.0.1/query-docs/'
          }
        ]
      },
      {
        text: 'Changelog',
        link: '/docs/changelog',
        activeMatch: '/changelog'
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
      '/docs/1.0.1-install': [
        {
          text: 'Installation Docs (1.0.1)',
          items: [
            {
              text: 'Installation',
              link: '/docs/1.0.1-install/',
              collapsed: true,
              items: [
                {
                  text: 'Docker',
                  link: '/docs/1.0.1-install#deploy-using-docker-compose'
                },
                {
                  text: 'Verify the installation',
                  link: '/docs/1.0.1-install#verify-the-installation'
                },
                {
                  text: 'Troubleshooting',
                  link: '/docs/1.0.1-install#troubleshooting'
                },
              ]
            },
            {
              text: 'Getting started',
              link: '/docs/1.0.1-install/getting-started',
            },
            {
              text: 'Configuration',
              link: '/docs/1.0.1-install/configuration'
            },
            {
              text: 'Data Lake Setup',
              link: '/docs/1.0.1-install/data-lake/introduction',
              items: [
                {
                  text: 'Introduction',
                  link: '/docs/1.0.1-install/data-lake/introduction',
                },
                {
                  text: 'Datasets',
                  link: '/docs/1.0.1-install/data-lake/datasets',
                  collapsed: true,
                  items: [
                    {
                      text: 'File Formats',
                      collapsed: true,
                      items: [
                        {
                          text: 'NetCDF',
                          link: '/docs/1.0.1-install/data-lake/datasets#netcdf'
                        },
                        {
                          text: 'ODV ASCII',
                          link: '/docs/1.0.1-install/data-lake/datasets#odv-ascii'
                        },
                        {
                          text: 'Parquet',
                          link: '/docs/1.0.1-install/data-lake/datasets#parquet'
                        },
                        {
                          text: 'CSV',
                          link: '/docs/1.0.1-install/data-lake/datasets#csv'
                        },
                        {
                          text: 'Arrow IPC',
                          link: '/docs/1.0.1-install/data-lake/datasets#arrow-ipc'
                        },
                        {
                          text: 'Beacon Binary Format',
                          link: '/docs/1.0.1-install/data-lake/datasets#beacon-binary-format'
                        },
                      ]
                    },
                    {
                      text: 'Managing Datasets',
                      link: '/docs/1.0.1-install/data-lake/datasets#managing-datasets',
                    },
                    {
                      text: 'Exploring Datasets',
                      link: '/docs/1.0.1-install/data-lake/datasets#exploring-datasets',
                    }
                  ]
                },
                {
                  text: 'Data Tables',
                  link: '/docs/1.0.1-install/data-lake/data-tables',
                  collapsed: true,
                  items: [
                    {
                      text: 'Logical Data Tables',
                      link: '/docs/1.0.1-install/data-lake/data-tables#logical-data-tables'
                    },
                    {
                      text: 'Physical Data Tables',
                      link: '/docs/1.0.1-install/data-lake/data-tables#physical-data-tables'
                    },
                    {
                      text: 'Table Extensions',
                      link: '/docs/1.0.1-install/data-lake/data-tables#table-extensions',
                      items: [
                        {
                          text: 'Temporal',
                          link: '/docs/1.0.1-install/data-lake/data-tables#temporal'
                        },
                        {
                          text: 'Vertical Axis',
                          link: '/docs/1.0.1-install/data-lake/data-tables#vertical-axis'
                        },
                        {
                          text: 'Geo Spatial',
                          link: '/docs/1.0.1-install/data-lake/data-tables#geo-spatial'
                        },
                        {
                          text: 'WMS',
                          link: '/docs/1.0.1-install/data-lake/data-tables#wms'
                        }
                      ]
                    },
                  ]
                },
              ]
            },
            {
              text: 'Using SQL',
              link: '/docs/1.0.1-install/sql'
            },
            {
              text: 'Tuning for performance',
              link: '/docs/1.0.1-install/performance',
              collapsed: true,
              items: [
                {
                  text: 'NetCDF',
                  link: '/docs/1.0.1-install/performance/netcdf'
                },
                {
                  text: 'Analyze Query Plan',
                  link: '/docs/1.0.1-install/performance/analyze-query'
                }
              ]
            },
            {
              text: 'Beacon Binary Format',
              link: '/docs/1.0.1-install/beacon-binary-format/',
              collapsed: true,
              items: [
                {
                  text: 'Import NetCDF',
                  link: '/docs/1.0.1-install/beacon-binary-format/import-netcdf',
                },
                {
                  text: 'Removing Datasets',
                  link: '/docs/1.0.1-install/beacon-binary-format/removing',
                },
                {
                  text: 'How to use',
                  link: '/docs/1.0.1-install/beacon-binary-format/usage'
                }
              ]
            },
            {
              text: 'Releases',
              link: '/docs/1.0.1-install/releases'
            },
            {
              text: 'Hardware Recommendations',
              link: '/docs/1.0.1-install/recommendations/hardware'
            },
            {
              text: 'Support',
              link: '/docs/1.0.1-install/support'
            }
          ]
        }
      ],
      '/docs/1.0.1/query-docs/': [
        {
          text: 'Query Docs (1.0.1)',
          items: [
            {
              text: 'Data Lake',
              link: '/docs/1.0.1/query-docs/data-lake#introduction-beacon-data-lake',
              items: [
                {
                  text: 'Introduction',
                  link: '/docs/1.0.1/query-docs/data-lake#introduction-beacon-data-lake',
                },
                {
                  text: 'Datasets',
                  link: '/docs/1.0.1/query-docs/data-lake#datasets'
                },
                {
                  text: 'Data Tables',
                  link: '/docs/1.0.1/query-docs/data-lake#data-tables'
                }
              ]
            },
            {
              text: 'Getting Started',
              link: '/docs/1.0.1/query-docs/getting-started',
            },
            {
              text: 'Exploring the Data Lake',
              link: '/docs/1.0.1/query-docs/exploring-data-lake',
              items: [

              ]
            },
            {
              text: 'Querying API',
              link: '/docs/1.0.1/query-docs/querying/introduction',
              items: [
                {
                  text: 'Introduction',
                  link: '/docs/1.0.1/query-docs/querying/introduction'
                },
                {
                  text: 'Querying with SQL',
                  link: '/docs/1.0.1/query-docs/querying/sql'
                },
                {
                  text: 'Querying with JSON',
                  link: '/docs/1.0.1/query-docs/querying/json',
                  items: [
                    {
                      text: 'Selecting Columns',
                      link: '/docs/1.0.1/query-docs/querying/json#query-parameters'
                    },
                    {
                      text: 'From',
                      link: '/docs/1.0.1/query-docs/querying/json#from'
                    },
                    {
                      text: 'Filtering',
                      link: '/docs/1.0.1/query-docs/querying/json#filters'
                    },
                    {
                      text: 'Output',
                      link: '/docs/1.0.1/query-docs/querying/json#output-format'
                    }
                  ]
                }
              ]
            },
            {
              text: 'Query Libraries',
              link: '/docs/1.0.1/query-docs/libraries/python',
              items: [
                {
                  text: 'Python',
                  link: '/docs/1.0.1/query-docs/libraries/python'
                }
              ]
            },
            {
              text: 'Studio (Web UI)',
              link: '/docs/1.0.1/query-docs/web-ui',
            },
          ]
        }
      ],
      '/docs/changelog': [
        {
          text: 'Changelog',
          items: [
            {
              text: '1.0.1 (latest)',
              link: '/docs/changelog'
            }
          ]
        }
      ]
    },

    socialLinks: [
      { icon: 'slack', link: 'https://join.slack.com/t/beacontechnic-wwa5548/shared_invite/zt-2dp1vv56r-tj_KFac0sAKNuAgUKPPDRg' },
      { icon: 'github', link: 'https://github.com/maris-development/beacon' },
    ]
  }
})
