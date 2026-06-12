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
            text: '1.7.0 (latest)',
            link: '/docs/1.7.0/introduction',
            activeMatch: '/docs/1.7.0/introduction'
          },
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
      '/docs/1.7.0/': [
        {
          text: 'Introduction',
          link: '/docs/1.7.0/introduction',
        },
        {
          text: 'Getting Started',
          link: '/docs/1.7.0/getting-started',
          collapsed: false,
          items: [
            {
              text: 'Local',
              link: '/docs/1.7.0/getting-started#local',
            },
            {
              text: 'S3 / Object Storage',
              link: '/docs/1.7.0/getting-started#s3-compatible-object-storage',
            }
          ]
        },
        {
          text: 'Connect',
          collapsed: false,
          items: [
            {
              text: 'JetBrains DataGrip',
              link: '/docs/1.7.0/connect/jetbrains-datagrip',
            },
            {
              text: 'Beacon CLI',
              link: '/docs/1.7.0/connect/beacon-cli',
            },
            {
              text: 'Beacon Python SDK',
              link: '/docs/1.7.0/connect/beacon-python-sdk',
            },
            {
              text: 'Python ADBC Driver',
              link: '/docs/1.7.0/connect/python-adbc',
            }
          ]
        },
        {
          text: 'Data Lakehouse Setup',
          link: '/docs/1.7.0/data-lake',
          collapsed: false,
          items: [
            {
              text: 'Supported Formats',
              link: '/docs/1.7.0/data-lake/datasets',
              collapsed: true,
              items: [
                {
                  text: 'Parquet',
                  link: '/docs/1.7.0/data-lake/datasets#parquet'
                },
                {
                  text: 'NetCDF',
                  link: '/docs/1.7.0/data-lake/datasets#netcdf'
                },
                {
                  text: 'Zarr',
                  link: '/docs/1.7.0/data-lake/datasets#zarr'
                },
                {
                  text: 'Arrow IPC',
                  link: '/docs/1.7.0/data-lake/datasets#arrow-ipc'
                },
                {
                  text: 'ODV ASCII',
                  link: '/docs/1.7.0/data-lake/datasets#odv-ascii'
                },
                {
                  text: 'CSV',
                  link: '/docs/1.7.0/data-lake/datasets#csv'
                },
                {
                  text: 'GeoTIFF',
                  link: '/docs/1.7.0/data-lake/datasets#geotiff--cloud-optimized-geotiff'
                },
                {
                  text: 'Atlas',
                  link: '/docs/1.7.0/data-lake/datasets#atlas'
                },
                {
                  text: 'Beacon Binary Format',
                  link: '/docs/1.7.0/data-lake/datasets#beacon-binary-format-bbf'
                },
              ]
            },
            {
              text: 'SQL Tables & Views',
              collapsed: false,
              items: [
                {
                  text: 'External Tables',
                  link: '/docs/1.7.0/data-lake/external-tables',
                  collapsed: true,
                  items: [
                    { text: 'Parquet', link: '/docs/1.7.0/data-lake/external-tables#parquet' },
                    { text: 'NetCDF', link: '/docs/1.7.0/data-lake/external-tables#netcdf' },
                    { text: 'Zarr', link: '/docs/1.7.0/data-lake/external-tables#zarr' },
                    { text: 'Atlas', link: '/docs/1.7.0/data-lake/external-tables#atlas' },
                    { text: 'CSV', link: '/docs/1.7.0/data-lake/external-tables#csv' },
                    { text: 'Arrow IPC', link: '/docs/1.7.0/data-lake/external-tables#arrow-ipc' },
                    { text: 'ODV ASCII', link: '/docs/1.7.0/data-lake/external-tables#odv-ascii' },
                    { text: 'GeoTIFF / COG', link: '/docs/1.7.0/data-lake/external-tables#geotiff-cog' },
                  ]
                },
                {
                  text: 'Views',
                  link: '/docs/1.7.0/data-lake/view',
                }
              ]
            },
            {
              text: 'Configuration',
              link: '/docs/1.7.0/data-lake/configuration',
            },
            {
              text: 'Performance Tuning',
              link: '/docs/1.7.0/data-lake/performance-tuning',
              collapsed: true,
              items: [
                {
                  text: 'Settings',
                  link: '/docs/1.7.0/data-lake/performance-tuning#beacon-query-engine-settings'
                },
                {
                  text: 'NetCDF',
                  link: '/docs/1.7.0/data-lake/performance-tuning#netcdf-tuning'
                },
                {
                  text: 'Zarr',
                  link: '/docs/1.7.0/data-lake/performance-tuning#zarr-predicate-pushdown'
                },
                {
                  text: 'Atlas',
                  link: '/docs/1.7.0/data-lake/performance-tuning#atlas-tuning'
                }
              ]
            }
          ]
        },
        {
          text: 'SQL Guide',
          link: '/docs/1.7.0/sql/',
          collapsed: true,
          items: [
            {
              text: 'Introduction',
              link: '/docs/1.7.0/sql/',
            },
            {
              text: 'Querying',
              collapsed: true,
              items: [
                {
                  text: 'SELECT',
                  link: '/docs/1.7.0/sql/select',
                },
                {
                  text: 'WHERE',
                  link: '/docs/1.7.0/sql/where',
                  collapsed: true,
                  items: [
                    { text: 'Comparison', link: '/docs/1.7.0/sql/where#comparison-operators' },
                    { text: 'BETWEEN', link: '/docs/1.7.0/sql/where#between' },
                    { text: 'IN', link: '/docs/1.7.0/sql/where#in' },
                    { text: 'LIKE', link: '/docs/1.7.0/sql/where#like' },
                    { text: 'IS NULL', link: '/docs/1.7.0/sql/where#is-null--is-not-null' },
                    { text: 'AND / OR / NOT', link: '/docs/1.7.0/sql/where#and--or--not' },
                    { text: 'Date & time', link: '/docs/1.7.0/sql/where#date-and-time-filtering' },
                  ]
                },
                {
                  text: 'GROUP BY',
                  link: '/docs/1.7.0/sql/group-by',
                },
                {
                  text: 'JOIN',
                  link: '/docs/1.7.0/sql/join',
                  collapsed: true,
                  items: [
                    { text: 'INNER JOIN', link: '/docs/1.7.0/sql/join#inner-join' },
                    { text: 'LEFT JOIN', link: '/docs/1.7.0/sql/join#left-join' },
                  ]
                },
                {
                  text: 'Reading Files',
                  link: '/docs/1.7.0/sql/table-functions',
                  collapsed: true,
                  items: [
                    { text: 'read_netcdf', link: '/docs/1.7.0/sql/table-functions#read_netcdf' },
                    { text: 'read_zarr', link: '/docs/1.7.0/sql/table-functions#read_zarr' },
                    { text: 'read_atlas', link: '/docs/1.7.0/sql/table-functions#read_atlas' },
                    { text: 'read_parquet', link: '/docs/1.7.0/sql/table-functions#read_parquet' },
                    { text: 'read_arrow', link: '/docs/1.7.0/sql/table-functions#read_arrow' },
                    { text: 'read_csv', link: '/docs/1.7.0/sql/table-functions#read_csv' },
                    { text: 'read_odv_ascii', link: '/docs/1.7.0/sql/table-functions#read_odv_ascii' },
                    { text: 'read_bbf', link: '/docs/1.7.0/sql/table-functions#read_bbf' },
                    { text: 'read_tiff', link: '/docs/1.7.0/sql/table-functions#read_tiff' },
                  ]
                },
                {
                  text: 'UNION ALL BY NAME',
                  link: '/docs/1.7.0/sql/union-by-name',
                },
              ]
            },
            {
              text: 'DDL',
              items: [
                {
                  text: 'CREATE EXTERNAL TABLE',
                  link: '/docs/1.7.0/sql/create-table',
                  collapsed: true,
                  items: [
                    { text: 'IF NOT EXISTS', link: '/docs/1.7.0/sql/create-table#if-not-exists' },
                    { text: 'OR REPLACE', link: '/docs/1.7.0/sql/create-table#or-replace' },
                    { text: 'PARTITIONED BY', link: '/docs/1.7.0/sql/create-table#partitioned-by' },
                    { text: 'DROP TABLE', link: '/docs/1.7.0/sql/create-table#drop-table' },
                  ]
                },
                {
                  text: 'CREATE TABLE (Managed)',
                  link: '/docs/1.7.0/sql/managed-tables',
                  collapsed: true,
                  items: [
                    { text: 'CREATE TABLE AS SELECT', link: '/docs/1.7.0/sql/managed-tables#create-table-as-select' },
                    { text: 'INSERT INTO', link: '/docs/1.7.0/sql/managed-tables#insert-into' },
                    { text: 'DELETE', link: '/docs/1.7.0/sql/managed-tables#delete' },
                    { text: 'UPDATE', link: '/docs/1.7.0/sql/managed-tables#update' },
                    { text: 'ALTER TABLE', link: '/docs/1.7.0/sql/managed-tables#alter-table' },
                    { text: 'DROP TABLE', link: '/docs/1.7.0/sql/managed-tables#drop-table' },
                  ]
                },
                {
                  text: 'CREATE VIEW',
                  link: '/docs/1.7.0/sql/create-view',
                },
                {
                  text: 'CREATE MATERIALIZED VIEW',
                  link: '/docs/1.7.0/sql/create-materialized-view',
                  collapsed: true,
                  items: [
                    { text: 'Querying', link: '/docs/1.7.0/sql/create-materialized-view#querying' },
                    { text: 'REFRESH', link: '/docs/1.7.0/sql/create-materialized-view#refresh' },
                    { text: 'DROP', link: '/docs/1.7.0/sql/create-materialized-view#drop' },
                  ]
                },
              ]
            },
            {
              text: 'Introspection',
              link: '/docs/1.7.0/sql/table-functions-utility',
              collapsed: true,
              items: [
                { text: 'read_schema', link: '/docs/1.7.0/sql/table-functions-utility#read_schema' },
                { text: 'list_datasets', link: '/docs/1.7.0/sql/table-functions-utility#list_datasets' },
                { text: 'view_dataset_statistics', link: '/docs/1.7.0/sql/table-functions-utility#view_dataset_statistics' },
                { text: 'view_external_table_statistics', link: '/docs/1.7.0/sql/table-functions-utility#view_external_table_statistics' },
                { text: 'view_statistics_cache', link: '/docs/1.7.0/sql/table-functions-utility#view_statistics_cache' },
              ]
            },
            {
              text: 'Function Reference',
              link: '/docs/1.7.0/sql/function-reference',
              collapsed: true,
              items: [
                { text: 'Aggregate', link: '/docs/1.7.0/sql/function-reference#aggregate-functions' },
                { text: 'Math', link: '/docs/1.7.0/sql/function-reference#math-functions' },
                { text: 'String', link: '/docs/1.7.0/sql/function-reference#string-functions' },
                { text: 'Regular Expressions', link: '/docs/1.7.0/sql/function-reference#regular-expression-functions' },
                { text: 'Binary String', link: '/docs/1.7.0/sql/function-reference#binary-string-functions' },
                { text: 'Date & Time', link: '/docs/1.7.0/sql/function-reference#date-and-time-functions' },
                { text: 'Conditional', link: '/docs/1.7.0/sql/function-reference#conditional-expressions' },
                { text: 'Casting', link: '/docs/1.7.0/sql/function-reference#casting' },
                {
                  text: 'Beacon-specific',
                  link: '/docs/1.7.0/sql/function-reference#beacon-specific-functions',
                  collapsed: true,
                  items: [
                    { text: 'beacon_version', link: '/docs/1.7.0/sql/function-reference#beacon_version' },
                    { text: 'coalesce_label', link: '/docs/1.7.0/sql/function-reference#coalesce_label' },
                    { text: 'cast_int8_as_char', link: '/docs/1.7.0/sql/function-reference#cast_int8_as_charn' },
                    { text: 'try_arrow_cast', link: '/docs/1.7.0/sql/function-reference#try_arrow_castexpr-type_str' },
                  ]
                },
                {
                  text: 'Geospatial',
                  link: '/docs/1.7.0/sql/function-reference#geospatial-functions',
                  collapsed: true,
                  items: [
                    { text: 'st_within_point', link: '/docs/1.7.0/sql/function-reference#st_within_point' },
                    { text: 'st_geojson_as_wkt', link: '/docs/1.7.0/sql/function-reference#st_geojson_as_wkt' },
                  ]
                },
                {
                  text: 'Domain Mapping',
                  link: '/docs/1.7.0/sql/function-reference#domain-mapping-functions',
                  collapsed: true,
                  items: [
                    { text: 'pressure_to_depth_teos_10', link: '/docs/1.7.0/sql/function-reference#pressure_to_depth_teos_10' },
                    { text: 'map_units', link: '/docs/1.7.0/sql/function-reference#map_units' },
                    { text: 'Common', link: '/docs/1.7.0/sql/function-reference#common' },
                    { text: 'CMEMS', link: '/docs/1.7.0/sql/function-reference#cmems' },
                    { text: 'CORA', link: '/docs/1.7.0/sql/function-reference#cora' },
                    { text: 'EMODnet Chemistry', link: '/docs/1.7.0/sql/function-reference#emodnet-chemistry' },
                    { text: 'SeaDataNet', link: '/docs/1.7.0/sql/function-reference#seadatanet' },
                    { text: 'Argo', link: '/docs/1.7.0/sql/function-reference#argo' },
                    { text: 'World Ocean Database', link: '/docs/1.7.0/sql/function-reference#world-ocean-database-wod' },
                  ]
                },
              ]
            },
          ]
        },
        {
          text: 'REST API',
          link: '/docs/1.7.0/api/',
          collapsed: true,
          items: [
            {
              text: 'Introduction',
              link: '/docs/1.7.0/api/',
            },
            {
              text: 'Exploring the Data Lake',
              link: '/docs/1.7.0/api/exploring-data-lake',
              collapsed: true,
              items: [
                { text: 'Datasets', link: '/docs/1.7.0/api/exploring-data-lake#datasets' },
                { text: 'Tables', link: '/docs/1.7.0/api/exploring-data-lake#tables' },
                { text: 'Functions', link: '/docs/1.7.0/api/exploring-data-lake#functions' },
                { text: 'Admin', link: '/docs/1.7.0/api/exploring-data-lake#admin-file-management' },
              ]
            },
            {
              text: 'Querying',
              link: '/docs/1.7.0/api/querying',
              collapsed: false,
              items: [
                {
                  text: 'JSON Query DSL',
                  link: '/docs/1.7.0/api/querying/json',
                  collapsed: true,
                  items: [
                    { text: 'Selecting Columns', link: '/docs/1.7.0/api/querying/json#selecting-columns' },
                    { text: 'Data Source', link: '/docs/1.7.0/api/querying/json#choosing-the-data-source-from' },
                    { text: 'Filters', link: '/docs/1.7.0/api/querying/json#filters' },
                    { text: 'Sorting & Pagination', link: '/docs/1.7.0/api/querying/json#sorting-and-pagination' },
                    { text: 'Output Formats', link: '/docs/1.7.0/api/querying/json#output-formats' },
                  ]
                },
                {
                  text: 'SQL',
                  link: '/docs/1.7.0/api/querying/sql',
                  collapsed: true,
                  items: [
                    { text: 'Query a Table', link: '/docs/1.7.0/api/querying/sql#query-a-registered-table' },
                    { text: 'Table Functions', link: '/docs/1.7.0/api/querying/sql#query-files-directly' },
                    { text: 'Output Formats', link: '/docs/1.7.0/api/querying/sql#output-formats' },
                  ]
                },
                {
                  text: 'Examples',
                  link: '/docs/1.7.0/api/querying/examples',
                },
              ]
            },
          ]
        }
      ],
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
          text: 'Release Posts',
          items: [
            {
              text: 'What\'s new in 1.7.0',
              link: '/docs/changelog/release-1.7.0'
            },
            {
              text: 'What\'s new in 1.6.0',
              link: '/docs/changelog/release-1.6.0'
            },
          ]
        },
        {
          text: 'Changelog',
          items: [
            {
              text: '1.7.0',
              link: '/docs/changelog'
            },
            {
              text: '1.6.1',
              link: '/docs/changelog'
            },
            {
              text: '1.6.0',
              link: '/docs/changelog'
            },
            {
              text: '1.5.4',
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
