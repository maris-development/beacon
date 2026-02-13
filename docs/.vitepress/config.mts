import { defineConfig } from 'vitepress'

// https://vitepress.dev/reference/site-config
export default defineConfig({
  markdown: {
    theme: {
      dark: 'dark-plus',
      light: 'light-plus',
    }
  },
  head: [['link', { rel: 'icon', href: '/beacon/favicon.ico' }]],
  base: '/beacon/',
  ignoreDeadLinks: true,
  title: "Beacon",
  description: "Beacon Documentation",
  lastUpdated: true,
  themeConfig: {
    search: {
      provider: 'local'
    },
    // https://vitepress.dev/reference/default-theme-config
    logo: '/beacon-logo-small.png',
    nav: [
      { text: 'Beacon', link: 'https://beacon.maris.nl/', target: '_blank', rel: 'noopener' },
      {
        text: 'Docs', items: [
          {
            text: '1.5.2 (latest)',
            link: '/docs/1.5.2/introduction',
            activeMatch: '/docs/1.5.2/introduction'
          },
        ]
      },
      {
        text: 'Changelog',
        link: '/docs/changelog',
        activeMatch: '/changelog'
      },
      {
        text: 'Blog',
        link: '/blog/world-ocean-database-node',
        activeMatch: '/blog/'
      },
      {
        text: 'Use Cases',
        activeMatch: '/use-cases/',
        items: [
          {
            text: '🌍 World Ocean Database (NetCDF)',
            link: '/use-cases/world-ocean-database',
          },
          {
            text: '🌊 Blue Cloud 2026',
            link: '/use-cases/blue-cloud-2026',
          },
          {
            text: '🌡️ C3S ERA5 Daily (Zarr)',
            link: '/use-cases/c3s-era5-daily',
          },
          {
            text: '🐟 Informatiehuis Marien (Parquet)',
            link: '/use-cases/informatiehuis-marien',
          },
          {
            text: '🗺️ EOSC-Future',
            link: '/use-cases/eosc-future',
          }
        ]
      }
    ],

    sidebar: {
      '/docs/1.5.2/': [
        {
          text: 'Introduction',
          link: '/docs/1.5.2/introduction',
        },
        {
          text: 'Getting Started',
          link: '/docs/1.5.2/getting-started',
          collapsed: false,
          items: [
            {
              text: 'Local',
              link: '/docs/1.5.2/getting-started#local',
            },
            {
              text: 'Cloud (S3)',
              link: '/docs/1.5.2/getting-started#s3-compatible-object-storage',
            }
          ]
        },
        {
          text: 'Data Lake Setup',
          link: '/docs/1.5.2/data-lake',
          collapsed: false,
          items: [
            {
              text: 'Datasets',
              link: '/docs/1.5.2/data-lake/datasets',
              items: [
                {
                  text: 'Zarr',
                  link: '/docs/1.5.2/data-lake/datasets#zarr'
                },
                {
                  text: 'NetCDF',
                  link: '/docs/1.5.2/data-lake/datasets#netcdf'
                },
                {
                  text: 'ODV ASCII',
                  link: '/docs/1.5.2/data-lake/datasets#odv-ascii'
                },
                {
                  text: 'Parquet',
                  link: '/docs/1.5.2/data-lake/datasets#parquet'
                },
                {
                  text: 'CSV',
                  link: '/docs/1.5.2/data-lake/datasets#csv'
                },
                {
                  text: 'Arrow IPC',
                  link: '/docs/1.5.2/data-lake/datasets#arrow-ipc'
                },
                {
                  text: 'Beacon Binary Format',
                  link: '/docs/1.5.2/data-lake/datasets#beacon-binary-format'
                },
              ]
            },
            {
              text: 'Collections',
              link: '/docs/1.5.2/data-lake/collections',
              collapsed: true,
              items: [
                {
                  text: 'Logical Collections',
                  link: '/docs/1.5.2/data-lake/collections#logical-data-tables'
                },
                {
                  text: 'Preset Collections',
                  link: '/docs/1.5.2/data-lake/collections#preset-data-tables'
                }
              ]
            },
            {
              text: 'Configuration',
              link: '/docs/1.5.2/data-lake/configuration',
            },
            {
              text: 'Performance Tuning',
              link: '/docs/1.5.2/data-lake/performance-tuning',
              collapsed: true,
              items: [
                {
                  text: 'Settings',
                  link: '/docs/1.5.2/data-lake/performance-tuning#beacon-query-engine-settings'
                },
                {
                  text: 'NetCDF',
                  link: '/docs/1.5.2/data-lake/performance-tuning#netcdf-tuning'
                },
                {
                  text: 'Zarr',
                  link: '/docs/1.5.2/data-lake/performance-tuning#zarr-statistics-predicate-pruning'
                }
              ]
            }
          ]
        },
        {
          text: 'API',
          link: '/docs/1.5.2/api/',
          collapsed: false,
          items: [
            {
              text: 'Introduction',
              link: '/docs/1.5.2/api/',
            },
            {
              text: 'Exploring the Data Lake',
              link: '/docs/1.5.2/api/exploring-data-lake',
            },
            {
              text: 'Querying',
              link: '/docs/1.5.2/api/querying',
              collapsed: false,
              items: [
                {
                  text: 'SQL',
                  link: '/docs/1.5.2/api/querying/sql',
                },
                {
                  text: 'JSON',
                  link: '/docs/1.5.2/api/querying/json',
                  items: [
                    {
                      text: 'Selecting Columns',
                      link: '/docs/1.5.2/api/querying/json#selecting-columns'
                    },
                    {
                      text: 'From',
                      link: '/docs/1.5.2/api/querying/json#choosing-the-data-source-from'
                    },
                    {
                      text: 'Filtering',
                      link: '/docs/1.5.2/api/querying/json#filters'
                    },
                    {
                      text: 'Output',
                      link: '/docs/1.5.2/api/querying/json#output-formats'
                    }
                  ]
                },
                {
                  text: 'Examples',
                  link: '/docs/1.5.2/api/querying/examples',
                },
              ]
            },
          ]

        },
        {
          text: 'API Libraries & Tooling',
          link: '/docs/1.5.2/libraries-tooling',
          items: [
            {
              text: 'Python',
              link: '/docs/1.5.2/libraries-tooling/python-sdk',
            },
            {
              text: 'CLI Tool',
              link: '/docs/1.5.2/libraries-tooling/cli',
            }
          ]
        }
      ],
      '/blog/': [
        {
          text: 'Introducing The World Ocean Database Node in Beacon',
          link: '/blog/world-ocean-database-node',
          collapsed: false,
          items: [
          ]
        },
        {
          text: 'NetCDF Performance Tuning in Beacon 1.5',
          link: '/blog/netcdf-performance-tuning',
          collapsed: false,
          items: [
          ]
        },
        {
          text: 'Introducing Zarr Support in Beacon 1.4',
          link: '/blog/zarr-support',
          collapsed: false,
          items: [
          ]
        },
        {
          text: 'Beacon Python SDK: Simplifying Data Lake Queries',
          link: '/blog/beacon-python-sdk',
          collapsed: false,
          items: [
          ]
        },
        {
          text: 'Beacon 1.3 Released: New Features and Improvements',
          link: '/blog/beacon-1-3-released',
          collapsed: false,
          items: [
          ]
        }
      ],
      '/use-cases/': [
        {
          text: 'Use Cases',
          link: '/use-cases/',
          collapsed: false,
          items: [
            {
              text: '🌍 World Ocean Database (NetCDF)',
              link: '/use-cases/world-ocean-database',
            },
            {
              text: '🌊 Blue Cloud 2026',
              link: '/use-cases/blue-cloud-2026',
              collapsed: false,
              items: [
                {
                  text: 'ARGO Floats',
                  link: '/use-cases/blue-cloud-2026#argo-floats'
                },
                {
                  text: 'CMEMS CORA',
                  link: '/use-cases/blue-cloud-2026#cmems-cora-in-situ'
                },
                {
                  text: 'SeaDataNet',
                  link: '/use-cases/blue-cloud-2026#seadatanet'
                },
                {
                  text: 'World Ocean Database',
                  link: '/use-cases/blue-cloud-2026#world-ocean-database'
                },
                {
                  text: 'EMODnet Chemistry',
                  link: '/use-cases/blue-cloud-2026#emodnet-chemistry-eutrophication'
                },
                {
                  text: 'CMEMS BGC',
                  link: '/use-cases/blue-cloud-2026#cmems-bgc-in-situ'
                }
              ]
            },
            {
              text: '🌡️ C3S ERA5 Daily (Zarr)',
              link: '/use-cases/c3s-era5-daily',
            },
            {
              text: '🐟 Informatiehuis Marien (Parquet)',
              link: '/use-cases/informatiehuis-marien',
            },
            {
              text: '🗺️ EOSC-Future',
              link: '/use-cases/eosc-future',
            }
          ]
        }
      ],
      // '/available-nodes/': [
      //   {
      //     text: 'Euro-Argo',
      //     link: '/available-nodes/available-nodes#euro-argo',
      //   },
      //   {
      //     text: 'World Ocean Database',
      //     link: '/available-nodes/available-nodes#world-ocean-database',
      //   },
      //   {
      //     text: 'CMEMS CORA',
      //     link: '/available-nodes/available-nodes#cora-profiles-time-series',
      //   },
      //   {
      //     text: 'Access token',
      //     link: '/available-nodes/available-nodes#obtain-personal-access-token',
      //   }
      // ],
      '/docs/1.5.0-install': [{
        text: 'Installation Docs (1.5.0)',
        items: [
          {
            text: 'Introduction',
            link: '/docs/1.5.0-install/'
          },
          {
            text: 'Installation',
            link: '/docs/1.5.0-install/installation',
            collapsed: true,
            items: [
              {
                text: 'Docker',
                link: '/docs/1.5.0-install/installation#deploy-using-docker-compose'
              },
              {
                text: 'Verify the installation',
                link: '/docs/1.5.0-install/installation#verify-the-installation'
              },
              {
                text: 'Troubleshooting',
                link: '/docs/1.5.0-install/installation#troubleshooting'
              },
            ]
          },
          {
            text: 'Getting started',
            link: '/docs/1.5.0-install/getting-started',
            items: [
              {
                text: 'Local File System',
                link: '/docs/1.5.0-install/getting-started#beacon-local-file-system',
              },
              {
                text: 'S3 Cloud Storage (MinIO)',
                link: '/docs/1.5.0-install/getting-started#beacon-s3-cloud-storage',
              },
              {
                text: 'More Examples',
                link: '/docs/1.5.0-install/getting-started#ready-made-examples',
              },
            ]
          },
          {
            text: 'Configuration',
            link: '/docs/1.5.0-install/configuration'
          },
          {
            text: 'Data Lake Setup',
            link: '/docs/1.5.0-install/data-lake/introduction',
            items: [
              {
                text: 'Introduction',
                link: '/docs/1.5.0-install/data-lake/introduction',
              },
              {
                text: 'Datasets',
                link: '/docs/1.5.0-install/data-lake/datasets',
                collapsed: true,
                items: [
                  {
                    text: 'File Formats',
                    collapsed: true,
                    items: [
                      {
                        text: 'Zarr',
                        link: '/docs/1.5.0-install/data-lake/datasets#zarr'
                      },
                      {
                        text: 'NetCDF',
                        link: '/docs/1.5.0-install/data-lake/datasets#netcdf'
                      },
                      {
                        text: 'ODV ASCII',
                        link: '/docs/1.5.0-install/data-lake/datasets#odv-ascii'
                      },
                      {
                        text: 'Parquet',
                        link: '/docs/1.5.0-install/data-lake/datasets#parquet'
                      },
                      {
                        text: 'CSV',
                        link: '/docs/1.5.0-install/data-lake/datasets#csv'
                      },
                      {
                        text: 'Arrow IPC',
                        link: '/docs/1.5.0-install/data-lake/datasets#arrow-ipc'
                      },
                      {
                        text: 'Beacon Binary Format',
                        link: '/docs/1.5.0-install/data-lake/datasets#beacon-binary-format'
                      },
                    ]
                  },
                  {
                    text: 'Datasets Harmonization',
                    link: '/docs/1.5.0-install/data-lake/datasets-harmonization',
                    items: [
                      {
                        text: 'Schema Merging',
                        link: '/docs/1.5.0-install/data-lake/datasets-harmonization#schema-merging-overview'
                      },
                      {
                        text: 'N-Dimensional Datasets',
                        link: '/docs/1.5.0-install/data-lake/datasets-harmonization#n-dimensional-datasets-harmonization'
                      }
                    ]
                  },
                  {
                    text: 'Managing Datasets',
                    link: '/docs/1.5.0-install/data-lake/datasets#managing-datasets',
                  },
                  {
                    text: 'Exploring Datasets',
                    link: '/docs/1.5.0-install/data-lake/datasets#exploring-datasets',
                  }
                ]
              },
              {
                text: 'Data Tables',
                link: '/docs/1.5.0-install/data-lake/data-tables',
                collapsed: true,
                items: [
                  {
                    text: 'Logical Data Tables',
                    link: '/docs/1.5.0-install/data-lake/data-tables#logical-data-tables'
                  },
                  {
                    text: 'Preset Data Tables',
                    link: '/docs/1.5.0-install/data-lake/data-tables#preset-data-tables'
                  },
                ]
              },
              {
                text: 'Python SDK',
                link: '/docs/1.5.0-install/data-lake/python-sdk'
              },
              {
                text: 'CLI Tool',
                link: '/docs/1.5.0-install/data-lake/cli-tool'
              }
            ]
          },
          {
            text: 'Data Lake Querying',
            link: '/docs/1.5.0-install/data-lake-querying',
            items: [
              {
                text: 'Introduction',
                link: '/docs/1.5.0-install/data-lake-querying'
              },
              {
                text: 'Querying with SQL',
                link: '/docs/1.5.0-install/data-lake-querying/sql',
              },
              {
                text: 'Querying with JSON',
                link: '/docs/1.5.0-install/data-lake-querying/json',
              }
            ]
          },
          {
            text: 'Tuning for performance',
            link: '/docs/1.5.0-install/performance',
            collapsed: false,
            items: [
              {
                text: 'NetCDF',
                link: '/docs/1.5.0-install/performance/netcdf'
              },
              {
                text: 'Analyze Query Plan',
                link: '/docs/1.5.0-install/performance/analyze-query'
              }
            ]
          },
          {
            text: 'Beacon Binary Format',
            link: '/docs/1.5.0-install/beacon-binary-format/',
            collapsed: true,
            items: [
              {
                text: 'About',
                link: '/docs/1.5.0-install/beacon-binary-format/',
              },
              {
                text: 'How to use (toolbox)',
                link: '/docs/1.5.0-install/beacon-binary-format/how-to-use',
              },
            ]
          },
          {
            text: 'Releases',
            link: '/docs/1.5.0-install/releases'
          },
          {
            text: 'Hardware Recommendations',
            link: '/docs/1.5.0-install/recommendations/hardware'
          },
          {
            text: 'Support',
            link: '/docs/1.5.0-install/support'
          }
        ]
      }],
      '/docs/1.5.0/query-docs/': [
        {
          text: 'Query Docs (1.5.0^)',
          items: [
            {
              text: 'Data Lake',
              link: '/docs/1.5.0/query-docs/data-lake#introduction-beacon-data-lake',
              items: [
                {
                  text: 'Introduction',
                  link: '/docs/1.5.0/query-docs/data-lake#introduction-beacon-data-lake',
                },
                {
                  text: 'Datasets',
                  link: '/docs/1.5.0/query-docs/data-lake#datasets'
                },
                {
                  text: 'Data Tables',
                  link: '/docs/1.5.0/query-docs/data-lake#data-tables'
                }
              ]
            },
            {
              text: 'Getting Started',
              link: '/docs/1.5.0/query-docs/getting-started#python',
              items: [
                {
                  text: 'Python',
                  link: '/docs/1.5.0/query-docs/getting-started#python'
                },
                {
                  text: 'Rest API',
                  link: '/docs/1.5.0/query-docs/getting-started#rest-api'
                },
              ]
            },
            {
              text: 'Exploring the Data Lake',
              link: '/docs/1.5.0/query-docs/exploring-data-lake',
              items: [

              ]
            },
            {
              text: 'Querying API',
              link: '/docs/1.5.0/query-docs/querying/introduction',
              items: [
                {
                  text: 'Introduction',
                  link: '/docs/1.5.0/query-docs/querying/introduction'
                },
                {
                  text: 'Querying with SQL',
                  link: '/docs/1.5.0/query-docs/querying/sql',
                  items: [
                    {
                      text: `Read Data Tables`,
                      link: '/docs/1.5.0/query-docs/querying/sql#query-data-tables-using-sql'
                    },
                    {
                      text: 'Read Zarr',
                      link: '/docs/1.5.0/query-docs/querying/sql#query-zarr-datasets-using-sql'
                    },
                    {
                      text: 'Read NetCDF',
                      link: '/docs/1.5.0/query-docs/querying/sql#query-netcdf-datasets-using-sql'
                    },
                    {
                      text: 'Read Parquet',
                      link: '/docs/1.5.0/query-docs/querying/sql#query-parquet-datasets-using-sql'
                    }
                  ]
                },
                {
                  text: 'Querying with JSON',
                  link: '/docs/1.5.0/query-docs/querying/json',
                  items: [
                    {
                      text: 'Selecting Columns',
                      link: '/docs/1.5.0/query-docs/querying/json#query-parameters'
                    },
                    {
                      text: 'From',
                      link: '/docs/1.5.0/query-docs/querying/json#from'
                    },
                    {
                      text: 'Filtering',
                      link: '/docs/1.5.0/query-docs/querying/json#filters'
                    },
                    {
                      text: 'Output',
                      link: '/docs/1.5.0/query-docs/querying/json#output-format'
                    }
                  ]
                }
              ]
            },
            {
              text: 'Query Libraries',
              link: '/docs/1.5.0/query-docs/libraries/python',
              items: [
                {
                  text: 'Python',
                  link: '/docs/1.5.0/query-docs/libraries/python'
                }
              ]
            },
            {
              text: 'Studio (Web UI)',
              link: '/docs/1.5.0/query-docs/web-ui',
            },
          ]
        }
      ],
      '/docs/changelog': [
        {
          text: 'Changelog',
          items: [
            {
              text: '1.5.2',
              link: '/docs/changelog'
            },
            {
              text: '1.5.0',
              link: '/docs/changelog'
            },
            {
              text: '1.4.0',
              link: '/docs/changelog'
            },
            {
              text: '1.3.0',
              link: '/docs/changelog'
            },
            {
              text: '1.2.0',
              link: '/docs/changelog'
            },
            {
              text: '1.0.1',
              link: '/docs/changelog'
            },
          ]
        },
        {
          text: 'Blue Cloud Changelog',
          items: [
            {
              text: '23-05-2025 (latest)',
              link: '/docs/changelog/blue-cloud'
            },
          ]
        },
      ]
    },
    socialLinks: [
      { icon: 'slack', link: 'https://join.slack.com/t/beacontechnic-wwa5548/shared_invite/zt-2dp1vv56r-tj_KFac0sAKNuAgUKPPDRg' },
      { icon: 'github', link: 'https://github.com/maris-development/beacon' },
    ]
  }
})
