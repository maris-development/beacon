import { defineConfig } from 'vitepress'
import { LATEST_VERSION, LATEST_ENTRY } from './theme/version.js'

const SITE_URL = 'https://maris-development.github.io/beacon/'
const OG_IMAGE = SITE_URL + 'hero.png'
const DESCRIPTION =
  'Beacon is an open-source query engine for scientific data — query NetCDF, Zarr, Parquet, GeoTIFF, CSV, ODV, Arrow and Atlas files in place over SQL and JSON APIs, on local storage or S3, with no download and no conversion.'

// Pages that are written but not released yet. They still build (so the team can
// review them via their direct URL), but they are kept out of the sidebar, the
// local search index, the sitemap, and search engines. To release one: delete it
// from this list, drop its `search: false` frontmatter, and uncomment the
// sidebar entries marked "MCP is unreleased" below.
const UNRELEASED_PAGES = [
  'docs/2.0.0-rc2/mcp.md'
]

const isUnreleased = (relativePath: string) =>
  UNRELEASED_PAGES.includes(relativePath)

const structuredData = [
  {
    '@context': 'https://schema.org',
    '@type': 'WebSite',
    name: 'Beacon',
    url: SITE_URL,
    description: DESCRIPTION
  },
  {
    '@context': 'https://schema.org',
    '@type': 'SoftwareApplication',
    name: 'Beacon',
    applicationCategory: 'DeveloperApplication',
    operatingSystem: 'Linux, macOS, Docker',
    description: DESCRIPTION,
    url: SITE_URL,
    license: 'https://www.gnu.org/licenses/agpl-3.0.html',
    offers: { '@type': 'Offer', price: '0', priceCurrency: 'USD' },
    sameAs: ['https://github.com/maris-development/beacon']
  }
]

// https://vitepress.dev/reference/site-config
export default defineConfig({
  markdown: {
    theme: {
      dark: 'dark-plus',
      light: 'light-plus',
    }
  },
  head: [
    ['link', { rel: 'icon', href: '/beacon/favicon.ico' }],
    ['meta', { name: 'author', content: 'MARIS' }],
    ['meta', { property: 'og:type', content: 'website' }],
    ['meta', { property: 'og:site_name', content: 'Beacon' }],
    ['meta', { property: 'og:image', content: OG_IMAGE }],
    ['meta', { name: 'twitter:card', content: 'summary_large_image' }],
    ['meta', { name: 'twitter:image', content: OG_IMAGE }],
    ['script', { type: 'application/ld+json' }, JSON.stringify(structuredData[0])],
    ['script', { type: 'application/ld+json' }, JSON.stringify(structuredData[1])],
  ],
  base: '/beacon/',
  ignoreDeadLinks: true,
  title: "Beacon",
  description: DESCRIPTION,
  lastUpdated: true,
  sitemap: {
    hostname: SITE_URL,
    transformItems: (items) =>
      items.filter(
        (item) => !UNRELEASED_PAGES.some((page) => item.url === page.replace(/\.md$/, '.html'))
      )
  },

  // No-JS fallback for the `/docs/latest` alias. The version comes from
  // theme/version.js, so bumping it there is the only edit needed.
  transformPageData(pageData) {
    if (pageData.relativePath === 'docs/latest/index.md') {
      const url = `/beacon/docs/${LATEST_VERSION}/${LATEST_ENTRY}`
      pageData.frontmatter.head ??= []
      pageData.frontmatter.head.push([
        'meta',
        { 'http-equiv': 'refresh', content: `0; url=${url}` }
      ])
    }
  },
  transformHead({ pageData }) {
    const path = pageData.relativePath
      .replace(/\.md$/, '.html')
      .replace(/(^|\/)index\.html$/, '$1')
    const url = SITE_URL + path
    const isHome =
      pageData.frontmatter?.layout === 'home' ||
      !pageData.title ||
      pageData.title === 'Beacon'
    const title = isHome
      ? 'Beacon — a query engine for scientific data'
      : `${pageData.title} | Beacon`
    const description =
      pageData.description || pageData.frontmatter?.description || DESCRIPTION
    const head: any[] = [
      ['link', { rel: 'canonical', href: url }],
      ['meta', { property: 'og:title', content: title }],
      ['meta', { property: 'og:description', content: description }],
      ['meta', { property: 'og:url', content: url }],
      ['meta', { name: 'twitter:title', content: title }],
      ['meta', { name: 'twitter:description', content: description }]
    ]
    if (isUnreleased(pageData.relativePath)) {
      head.push(['meta', { name: 'robots', content: 'noindex, nofollow' }])
    }
    return head
  },
  themeConfig: {
    search: {
      provider: 'local'
    },
    // https://vitepress.dev/reference/default-theme-config
    logo: '/beacon-logo-small.png',
    nav: [
      {
        text: 'Why Beacon',
        link: '/why-beacon',
        activeMatch: '/why-beacon'
      },
      {
        text: 'Docs', items: [
          {
            text: '2.0.0-rc2 (pre-release)',
            link: '/docs/2.0.0-rc2/introduction',
            activeMatch: '/docs/2.0.0-rc2/'
          },
          {
            text: '1.8.0 (latest)',
            link: '/docs/1.8.0/introduction',
            activeMatch: '/docs/1.8.0/introduction'
          },
          {
            text: '1.7.3',
            link: '/docs/1.7.3/introduction',
            activeMatch: '/docs/1.7.3/introduction'
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
      '/docs/2.0.0-rc2/': [
        {
          text: 'Overview',
          items: [
            { text: 'Introduction', link: '/docs/2.0.0-rc2/introduction' },
            {
              text: 'Quick Start',
              link: '/docs/2.0.0-rc2/quickstart',
              collapsed: true,
              items: [
                { text: 'Deploy a server', link: '/docs/2.0.0-rc2/quickstart#deploy-a-server' },
                { text: 'Query a server', link: '/docs/2.0.0-rc2/quickstart#query-a-server' },
              ]
            },
            { text: 'Concepts', link: '/docs/2.0.0-rc2/concepts' },
            { text: 'FAQ', link: '/docs/2.0.0-rc2/faq' },
          ]
        },
        {
          text: 'Deploy & Operate',
          collapsed: false,
          items: [
            {
              text: 'Getting Started',
              link: '/docs/2.0.0-rc2/getting-started',
              collapsed: true,
              items: [
                { text: 'Quick Start', link: '/docs/2.0.0-rc2/getting-started#quick-start' },
                { text: 'Local', link: '/docs/2.0.0-rc2/getting-started#local' },
                { text: 'S3 / Object Storage', link: '/docs/2.0.0-rc2/getting-started#s3-compatible-object-storage' },
              ]
            },
            { text: 'Configuration', link: '/docs/2.0.0-rc2/server/configuration' },
            { text: 'Access Control', link: '/docs/2.0.0-rc2/security/access-control' },
            { text: 'Performance Tuning', link: '/docs/2.0.0-rc2/server/performance-tuning' },
            { text: 'Storage internals', link: '/docs/2.0.0-rc2/internals/storage' },
            { text: 'File statistics', link: '/docs/2.0.0-rc2/internals/file-statistics' },
            { text: 'Troubleshooting', link: '/docs/2.0.0-rc2/troubleshooting' },
          ]
        },
        {
          text: 'Server Setup',
          collapsed: false,
          items: [
            { text: 'Overview', link: '/docs/2.0.0-rc2/server/' },
            {
              text: 'Datasets & Formats',
              link: '/docs/2.0.0-rc2/server/datasets',
              collapsed: true,
              items: [
                { text: 'All formats', link: '/docs/2.0.0-rc2/formats/' },
                { text: 'Inspect a schema', link: '/docs/2.0.0-rc2/formats/inspect-a-schema' },
                { text: 'Parquet', link: '/docs/2.0.0-rc2/formats/parquet' },
                { text: 'GeoParquet', link: '/docs/2.0.0-rc2/formats/geoparquet' },
                { text: 'CSV / TSV', link: '/docs/2.0.0-rc2/formats/csv' },
                { text: 'Arrow IPC', link: '/docs/2.0.0-rc2/formats/arrow' },
                { text: 'NetCDF', link: '/docs/2.0.0-rc2/formats/netcdf' },
                { text: 'HDF5', link: '/docs/2.0.0-rc2/formats/hdf5' },
                { text: 'Zarr', link: '/docs/2.0.0-rc2/formats/zarr' },
                { text: 'Atlas', link: '/docs/2.0.0-rc2/formats/atlas' },
                { text: 'GeoTIFF / COG', link: '/docs/2.0.0-rc2/formats/geotiff' },
                { text: 'BBF', link: '/docs/2.0.0-rc2/formats/bbf' },
                { text: 'Delta Lake', link: '/docs/2.0.0-rc2/formats/delta-lake' },
                { text: 'Apache Iceberg', link: '/docs/2.0.0-rc2/formats/iceberg' },
                { text: 'Icechunk', link: '/docs/2.0.0-rc2/formats/icechunk' },
                { text: 'ODV ASCII', link: '/docs/2.0.0-rc2/formats/odv' },
              ]
            },
            {
              text: 'Tables & Views',
              link: '/docs/2.0.0-rc2/data-sources/',
              collapsed: true,
              items: [
                { text: 'External Tables', link: '/docs/2.0.0-rc2/data-sources/external-tables' },
                { text: 'Managed Tables', link: '/docs/2.0.0-rc2/sql/managed-tables' },
                { text: 'Views', link: '/docs/2.0.0-rc2/server/view' },
                { text: 'Materialized Views', link: '/docs/2.0.0-rc2/sql/create-materialized-view' },
                { text: 'Crawlers', link: '/docs/2.0.0-rc2/server/crawlers' },
                { text: 'Extensions', link: '/docs/2.0.0-rc2/server/extensions' },
              ]
            },
            {
              text: 'Other Sources',
              collapsed: true,
              items: [
                { text: 'Object Storage (S3)', link: '/docs/2.0.0-rc2/data-sources/object-storage' },
                { text: 'SQL Databases', link: '/docs/2.0.0-rc2/data-sources/sql-databases' },
                { text: 'Remote Tables', link: '/docs/2.0.0-rc2/data-sources/remote-tables' },
                { text: 'ATTACH another server', link: '/docs/2.0.0-rc2/data-sources/attach' },
                { text: 'Secrets', link: '/docs/2.0.0-rc2/sql/secrets' },
              ]
            },
          ]
        },
        {
          text: 'SQL Reference',
          collapsed: true,
          items: [
            { text: 'Overview', link: '/docs/2.0.0-rc2/sql/' },
            { text: 'SELECT', link: '/docs/2.0.0-rc2/sql/select' },
            { text: 'WHERE', link: '/docs/2.0.0-rc2/sql/where' },
            { text: 'GROUP BY', link: '/docs/2.0.0-rc2/sql/group-by' },
            { text: 'JOIN', link: '/docs/2.0.0-rc2/sql/join' },
            { text: 'UNION BY NAME', link: '/docs/2.0.0-rc2/sql/union-by-name' },
            { text: 'CREATE TABLE', link: '/docs/2.0.0-rc2/sql/create-table' },
            { text: 'CREATE VIEW', link: '/docs/2.0.0-rc2/sql/create-view' },
            { text: 'CREATE MATERIALIZED VIEW', link: '/docs/2.0.0-rc2/sql/create-materialized-view' },
            { text: 'Managed Tables', link: '/docs/2.0.0-rc2/sql/managed-tables' },
            { text: 'Remote Tables & ATTACH', link: '/docs/2.0.0-rc2/sql/remote-tables' },
            { text: 'CREATE SECRET', link: '/docs/2.0.0-rc2/sql/secrets' },
            { text: 'SUMMARIZE', link: '/docs/2.0.0-rc2/sql/summarize' },
            { text: 'Table Functions', link: '/docs/2.0.0-rc2/sql/table-functions' },
            { text: 'Introspection', link: '/docs/2.0.0-rc2/sql/table-functions-utility' },
            { text: 'Function Reference', link: '/docs/2.0.0-rc2/sql/function-reference' },
            { text: 'Spatial Functions', link: '/docs/2.0.0-rc2/sql/spatial-functions' },
          ]
        },
        {
          text: 'REST API',
          collapsed: true,
          items: [
            { text: 'Overview', link: '/docs/2.0.0-rc2/api/' },
            { text: 'Querying', link: '/docs/2.0.0-rc2/api/querying/' },
            { text: 'SQL Queries', link: '/docs/2.0.0-rc2/api/querying/sql' },
            { text: 'JSON Queries', link: '/docs/2.0.0-rc2/api/querying/json' },
            { text: 'Examples', link: '/docs/2.0.0-rc2/api/querying/examples' },
            { text: 'Exploring the catalog', link: '/docs/2.0.0-rc2/api/exploring-data' },
          ]
        },
        {
          text: 'Connect',
          collapsed: true,
          items: [
            { text: 'Python SDK', link: '/docs/2.0.0-rc2/connect/python' },
            { text: 'TypeScript SDK', link: '/docs/2.0.0-rc2/connect/typescript' },
            { text: 'CLI', link: '/docs/2.0.0-rc2/connect/cli' },
            { text: 'Web Admin UI', link: '/docs/2.0.0-rc2/connect/web-admin-ui' },
            { text: 'DataGrip / JDBC', link: '/docs/2.0.0-rc2/connect/datagrip' },
            { text: 'Python ADBC', link: '/docs/2.0.0-rc2/connect/python-adbc' },
          ]
        },
        {
          text: 'Scientific Data',
          collapsed: true,
          items: [
            { text: 'How It Works', link: '/docs/2.0.0-rc2/how-it-works' },
            { text: 'Arrays to tables', link: '/docs/2.0.0-rc2/arrays-to-tables' },
            { text: 'CF decoding', link: '/docs/2.0.0-rc2/cf-decoding' },
            { text: 'Coming from xarray', link: '/docs/2.0.0-rc2/coming-from-xarray' },
            {
              text: 'Guides',
              link: '/docs/2.0.0-rc2/guides/',
              collapsed: true,
              items: [
                { text: 'Query a File Collection', link: '/docs/2.0.0-rc2/guides/query-a-collection' },
                { text: 'Query Data on S3', link: '/docs/2.0.0-rc2/guides/query-s3' },
                { text: 'Export Query Results', link: '/docs/2.0.0-rc2/guides/export-results' },
                { text: 'Speed Up Slow Queries', link: '/docs/2.0.0-rc2/guides/speed-up-queries' },
              ]
            },
          ]
        },
      ],
      '/docs/1.8.0/': [
        {
          text: 'Introduction',
          link: '/docs/1.8.0/introduction',
        },
        {
          text: 'Getting Started',
          link: '/docs/1.8.0/getting-started',
          collapsed: false,
          items: [
            {
              text: 'Quick Start',
              link: '/docs/1.8.0/getting-started#quick-start',
            },
            {
              text: 'Local',
              link: '/docs/1.8.0/getting-started#local',
            },
            {
              text: 'S3 / Object Storage',
              link: '/docs/1.8.0/getting-started#s3-compatible-object-storage',
            }
          ]
        },
        {
          text: 'Data Lakehouse Setup',
          link: '/docs/1.8.0/data-lake',
          collapsed: false,
          items: [
            {
              text: 'Supported Formats',
              link: '/docs/1.8.0/data-lake/datasets',
              collapsed: true,
              items: [
                {
                  text: 'Parquet',
                  link: '/docs/1.8.0/data-lake/datasets#parquet'
                },
                {
                  text: 'GeoParquet',
                  link: '/docs/1.8.0/data-lake/geoparquet'
                },
                {
                  text: 'NetCDF',
                  link: '/docs/1.8.0/data-lake/datasets#netcdf'
                },
                {
                  text: 'Zarr',
                  link: '/docs/1.8.0/data-lake/datasets#zarr'
                },
                {
                  text: 'Arrow IPC',
                  link: '/docs/1.8.0/data-lake/datasets#arrow-ipc'
                },
                {
                  text: 'ODV ASCII',
                  link: '/docs/1.8.0/data-lake/datasets#odv-ascii'
                },
                {
                  text: 'CSV',
                  link: '/docs/1.8.0/data-lake/datasets#csv'
                },
                {
                  text: 'GeoTIFF',
                  link: '/docs/1.8.0/data-lake/datasets#geotiff--cloud-optimized-geotiff'
                },
                {
                  text: 'Atlas',
                  link: '/docs/1.8.0/data-lake/datasets#atlas'
                },
                {
                  text: 'Beacon Binary Format',
                  link: '/docs/1.8.0/data-lake/datasets#beacon-binary-format-bbf'
                },
              ]
            },
            {
              text: 'SQL Tables & Views',
              collapsed: true,
              items: [
                {
                  text: 'External Tables',
                  link: '/docs/1.8.0/data-lake/external-tables',
                  collapsed: true,
                  items: [
                    { text: 'Parquet', link: '/docs/1.8.0/data-lake/external-tables#parquet' },
                    { text: 'GeoParquet', link: '/docs/1.8.0/data-lake/external-tables#geoparquet' },
                    { text: 'NetCDF', link: '/docs/1.8.0/data-lake/external-tables#netcdf' },
                    { text: 'Zarr', link: '/docs/1.8.0/data-lake/external-tables#zarr' },
                    { text: 'Atlas', link: '/docs/1.8.0/data-lake/external-tables#atlas' },
                    { text: 'CSV', link: '/docs/1.8.0/data-lake/external-tables#csv' },
                    { text: 'Arrow IPC', link: '/docs/1.8.0/data-lake/external-tables#arrow-ipc' },
                    { text: 'ODV ASCII', link: '/docs/1.8.0/data-lake/external-tables#odv-ascii' },
                    { text: 'GeoTIFF / COG', link: '/docs/1.8.0/data-lake/external-tables#geotiff-cog' },
                    { text: 'Delta Lake', link: '/docs/1.8.0/data-lake/external-tables#delta-lake' },
                  ]
                },
                {
                  text: 'Crawlers',
                  link: '/docs/1.8.0/data-lake/crawlers',
                  collapsed: true,
                  items: [
                    { text: 'CREATE CRAWLER', link: '/docs/1.8.0/data-lake/crawlers#create-crawler' },
                    { text: 'RUN / SHOW / DROP', link: '/docs/1.8.0/data-lake/crawlers#run-crawler' },
                    { text: 'Partition detection', link: '/docs/1.8.0/data-lake/crawlers#partition-detection' },
                    { text: 'Triggers', link: '/docs/1.8.0/data-lake/crawlers#triggers' },
                    { text: 'Configuration', link: '/docs/1.8.0/data-lake/crawlers#configuration' },
                    { text: 'Limitations', link: '/docs/1.8.0/data-lake/crawlers#supported-formats-and-limitations' },
                  ]
                },
                {
                  text: 'Managed Tables',
                  link: '/docs/1.8.0/sql/managed-tables',
                },
                {
                  text: 'Views',
                  link: '/docs/1.8.0/data-lake/view',
                },
                {
                  text: 'Delta Lake',
                  link: '/docs/1.8.0/data-lake/delta-lake',
                  collapsed: true,
                  items: [
                    { text: 'Defining a Delta table', link: '/docs/1.8.0/data-lake/delta-lake#defining-a-delta-external-table' },
                    { text: 'Time travel', link: '/docs/1.8.0/data-lake/delta-lake#options-time-travel' },
                    { text: 'Writing (INSERT INTO)', link: '/docs/1.8.0/data-lake/delta-lake#writing-insert-into' },
                    { text: 'Limitations', link: '/docs/1.8.0/data-lake/delta-lake#limitations' },
                  ]
                },
                {
                  text: 'Remote Tables (Federation)',
                  link: '/docs/1.8.0/data-lake/remote-tables',
                  collapsed: true,
                  items: [
                    { text: 'Defining a remote table', link: '/docs/1.8.0/data-lake/remote-tables#defining-a-remote-table' },
                    { text: 'How pushdown works', link: '/docs/1.8.0/data-lake/remote-tables#how-pushdown-works' },
                    { text: 'Schema handling', link: '/docs/1.8.0/data-lake/remote-tables#schema-handling' },
                    { text: 'Limitations', link: '/docs/1.8.0/data-lake/remote-tables#limitations' },
                  ]
                },
                {
                  text: 'SQL Databases (Postgres / MySQL)',
                  link: '/docs/1.8.0/data-lake/sql-databases',
                  collapsed: true,
                  items: [
                    { text: 'Credentials', link: '/docs/1.8.0/data-lake/sql-databases#credentials' },
                    { text: 'Defining a database table', link: '/docs/1.8.0/data-lake/sql-databases#defining-a-sql-database-table' },
                    { text: 'How pushdown works', link: '/docs/1.8.0/data-lake/sql-databases#how-pushdown-works' },
                    { text: 'Limitations', link: '/docs/1.8.0/data-lake/sql-databases#limitations' },
                  ]
                }
              ]
            },
            {
              text: 'Configuration',
              link: '/docs/1.8.0/data-lake/configuration',
            },
            {
              text: 'Access Control',
              link: '/docs/1.8.0/security/access-control',
              collapsed: true,
              items: [
                { text: 'Enforcement', link: '/docs/1.8.0/security/access-control#enforcement' },
                { text: 'Roles & privileges', link: '/docs/1.8.0/security/access-control#roles-privileges-and-targets' },
                { text: 'OIDC (SSO)', link: '/docs/1.8.0/security/access-control#oidc-single-sign-on' },
                { text: 'Configuration', link: '/docs/1.8.0/security/access-control#configuration-reference' },
              ]
            },
            {
              text: 'Performance Tuning',
              link: '/docs/1.8.0/data-lake/performance-tuning',
              collapsed: true,
              items: [
                {
                  text: 'Settings',
                  link: '/docs/1.8.0/data-lake/performance-tuning#beacon-query-engine-settings'
                },
                {
                  text: 'NetCDF',
                  link: '/docs/1.8.0/data-lake/performance-tuning#netcdf-tuning'
                },
                {
                  text: 'Zarr',
                  link: '/docs/1.8.0/data-lake/performance-tuning#zarr-predicate-pushdown'
                },
                {
                  text: 'Atlas',
                  link: '/docs/1.8.0/data-lake/performance-tuning#atlas-tuning'
                }
              ]
            },
            // Table Extensions / "Serve to AI Agents (MCP)" were added to the 1.8.0
            // folder after the 1.8.0 release; the folder is now pinned to the
            // v1.8.0 tag, so those pages no longer exist here. They live in 2.0.0.
          ]
        },
        {
          text: 'SQL Guide',
          link: '/docs/1.8.0/sql/',
          collapsed: true,
          items: [
            {
              text: 'Introduction',
              link: '/docs/1.8.0/sql/',
            },
            {
              text: 'Querying',
              collapsed: true,
              items: [
                {
                  text: 'SELECT',
                  link: '/docs/1.8.0/sql/select',
                },
                {
                  text: 'WHERE',
                  link: '/docs/1.8.0/sql/where',
                  collapsed: true,
                  items: [
                    { text: 'Comparison', link: '/docs/1.8.0/sql/where#comparison-operators' },
                    { text: 'BETWEEN', link: '/docs/1.8.0/sql/where#between' },
                    { text: 'IN', link: '/docs/1.8.0/sql/where#in' },
                    { text: 'LIKE', link: '/docs/1.8.0/sql/where#like' },
                    { text: 'IS NULL', link: '/docs/1.8.0/sql/where#is-null--is-not-null' },
                    { text: 'AND / OR / NOT', link: '/docs/1.8.0/sql/where#and--or--not' },
                    { text: 'Date & time', link: '/docs/1.8.0/sql/where#date-and-time-filtering' },
                  ]
                },
                {
                  text: 'GROUP BY',
                  link: '/docs/1.8.0/sql/group-by',
                },
                {
                  text: 'JOIN',
                  link: '/docs/1.8.0/sql/join',
                  collapsed: true,
                  items: [
                    { text: 'INNER JOIN', link: '/docs/1.8.0/sql/join#inner-join' },
                    { text: 'LEFT JOIN', link: '/docs/1.8.0/sql/join#left-join' },
                  ]
                },
                {
                  text: 'Reading Files',
                  link: '/docs/1.8.0/sql/table-functions',
                  collapsed: true,
                  items: [
                    { text: 'read_netcdf', link: '/docs/1.8.0/sql/table-functions#read_netcdf' },
                    { text: 'read_zarr', link: '/docs/1.8.0/sql/table-functions#read_zarr' },
                    { text: 'read_atlas', link: '/docs/1.8.0/sql/table-functions#read_atlas' },
                    { text: 'read_parquet', link: '/docs/1.8.0/sql/table-functions#read_parquet' },
                    { text: 'read_arrow', link: '/docs/1.8.0/sql/table-functions#read_arrow' },
                    { text: 'read_csv', link: '/docs/1.8.0/sql/table-functions#read_csv' },
                    { text: 'read_odv_ascii', link: '/docs/1.8.0/sql/table-functions#read_odv_ascii' },
                    { text: 'read_bbf', link: '/docs/1.8.0/sql/table-functions#read_bbf' },
                    { text: 'read_tiff', link: '/docs/1.8.0/sql/table-functions#read_tiff' },
                    { text: 'read_delta', link: '/docs/1.8.0/sql/table-functions#read_delta' },
                  ]
                },
                {
                  text: 'UNION ALL BY NAME',
                  link: '/docs/1.8.0/sql/union-by-name',
                },
                {
                  text: 'GeoParquet',
                  link: '/docs/1.8.0/sql/geoparquet',
                },
                {
                  text: 'Remote Tables',
                  link: '/docs/1.8.0/sql/remote-tables',
                },
              ]
            },
            {
              text: 'DDL',
              collapsed: true,
              items: [
                {
                  text: 'CREATE EXTERNAL TABLE',
                  link: '/docs/1.8.0/sql/create-table',
                  collapsed: true,
                  items: [
                    { text: 'IF NOT EXISTS', link: '/docs/1.8.0/sql/create-table#if-not-exists' },
                    { text: 'OR REPLACE', link: '/docs/1.8.0/sql/create-table#or-replace' },
                    { text: 'PARTITIONED BY', link: '/docs/1.8.0/sql/create-table#partitioned-by' },
                    { text: 'DROP TABLE', link: '/docs/1.8.0/sql/create-table#drop-table' },
                  ]
                },
                {
                  text: 'CREATE TABLE (Managed)',
                  link: '/docs/1.8.0/sql/managed-tables',
                  collapsed: true,
                  items: [
                    { text: 'Storage engine', link: '/docs/1.8.0/sql/managed-tables#choosing-the-storage-engine' },
                    { text: 'CREATE TABLE AS SELECT', link: '/docs/1.8.0/sql/managed-tables#create-table-as-select' },
                    { text: 'INSERT INTO', link: '/docs/1.8.0/sql/managed-tables#insert-into' },
                    { text: 'DELETE', link: '/docs/1.8.0/sql/managed-tables#delete' },
                    { text: 'UPDATE', link: '/docs/1.8.0/sql/managed-tables#update' },
                    { text: 'ALTER TABLE', link: '/docs/1.8.0/sql/managed-tables#alter-table' },
                    { text: 'Indexes', link: '/docs/1.8.0/sql/managed-tables#indexes' },
                    { text: 'DROP TABLE', link: '/docs/1.8.0/sql/managed-tables#drop-table' },
                  ]
                },
                {
                  text: 'CREATE VIEW',
                  link: '/docs/1.8.0/sql/create-view',
                },
                {
                  text: 'CREATE MATERIALIZED VIEW',
                  link: '/docs/1.8.0/sql/create-materialized-view',
                  collapsed: true,
                  items: [
                    { text: 'Querying', link: '/docs/1.8.0/sql/create-materialized-view#querying' },
                    { text: 'REFRESH', link: '/docs/1.8.0/sql/create-materialized-view#refresh' },
                    { text: 'DROP', link: '/docs/1.8.0/sql/create-materialized-view#drop' },
                  ]
                },
                {
                  text: 'CREATE CRAWLER',
                  link: '/docs/1.8.0/data-lake/crawlers',
                  collapsed: true,
                  items: [
                    { text: 'Options', link: '/docs/1.8.0/data-lake/crawlers#options' },
                    { text: 'RUN CRAWLER', link: '/docs/1.8.0/data-lake/crawlers#run-crawler' },
                    { text: 'SHOW CRAWLERS', link: '/docs/1.8.0/data-lake/crawlers#show-crawlers' },
                    { text: 'DROP CRAWLER', link: '/docs/1.8.0/data-lake/crawlers#drop-crawler' },
                  ]
                },
                {
                  text: 'Users, Roles & Grants',
                  link: '/docs/1.8.0/security/access-control#managing-users-and-roles-sql',
                  collapsed: true,
                  items: [
                    { text: 'CREATE USER', link: '/docs/1.8.0/security/access-control#users' },
                    { text: 'CREATE ROLE', link: '/docs/1.8.0/security/access-control#roles' },
                    { text: 'GRANT / DENY', link: '/docs/1.8.0/security/access-control#grants-and-denies' },
                    { text: 'Anonymous access', link: '/docs/1.8.0/security/access-control#anonymous-access' },
                  ]
                },
              ]
            },
            {
              text: 'Introspection',
              link: '/docs/1.8.0/sql/table-functions-utility',
              collapsed: true,
              items: [
                { text: 'read_schema', link: '/docs/1.8.0/sql/table-functions-utility#read_schema' },
                { text: 'list_datasets', link: '/docs/1.8.0/sql/table-functions-utility#list_datasets' },
                { text: 'view_dataset_statistics', link: '/docs/1.8.0/sql/table-functions-utility#view_dataset_statistics' },
                { text: 'view_external_table_statistics', link: '/docs/1.8.0/sql/table-functions-utility#view_external_table_statistics' },
                { text: 'view_statistics_cache', link: '/docs/1.8.0/sql/table-functions-utility#view_statistics_cache' },
              ]
            },
            {
              text: 'Function Reference',
              link: '/docs/1.8.0/sql/function-reference',
              collapsed: true,
              items: [
                { text: 'Aggregate', link: '/docs/1.8.0/sql/function-reference#aggregate-functions' },
                { text: 'Math', link: '/docs/1.8.0/sql/function-reference#math-functions' },
                { text: 'String', link: '/docs/1.8.0/sql/function-reference#string-functions' },
                { text: 'Regular Expressions', link: '/docs/1.8.0/sql/function-reference#regular-expression-functions' },
                { text: 'Binary String', link: '/docs/1.8.0/sql/function-reference#binary-string-functions' },
                { text: 'Date & Time', link: '/docs/1.8.0/sql/function-reference#date-and-time-functions' },
                { text: 'Conditional', link: '/docs/1.8.0/sql/function-reference#conditional-expressions' },
                { text: 'Casting', link: '/docs/1.8.0/sql/function-reference#casting' },
                {
                  text: 'Beacon-specific',
                  link: '/docs/1.8.0/sql/function-reference#beacon-specific-functions',
                  collapsed: true,
                  items: [
                    { text: 'beacon_version', link: '/docs/1.8.0/sql/function-reference#beacon_version' },
                    { text: 'coalesce_label', link: '/docs/1.8.0/sql/function-reference#coalesce_label' },
                    { text: 'cast_int8_as_char', link: '/docs/1.8.0/sql/function-reference#cast_int8_as_charn' },
                    { text: 'try_arrow_cast', link: '/docs/1.8.0/sql/function-reference#try_arrow_castexpr-type_str' },
                  ]
                },
                {
                  text: 'Geospatial',
                  link: '/docs/1.8.0/sql/function-reference#geospatial-functions',
                  collapsed: true,
                  items: [
                    { text: 'st_within_point', link: '/docs/1.8.0/sql/function-reference#st_within_point' },
                    { text: 'st_geojson_as_wkt', link: '/docs/1.8.0/sql/function-reference#st_geojson_as_wkt' },
                  ]
                },
                {
                  text: 'Domain Mapping',
                  link: '/docs/1.8.0/sql/function-reference#domain-mapping-functions',
                  collapsed: true,
                  items: [
                    { text: 'pressure_to_depth_teos_10', link: '/docs/1.8.0/sql/function-reference#pressure_to_depth_teos_10' },
                    { text: 'map_units', link: '/docs/1.8.0/sql/function-reference#map_units' },
                    { text: 'Common', link: '/docs/1.8.0/sql/function-reference#common' },
                    { text: 'CMEMS', link: '/docs/1.8.0/sql/function-reference#cmems' },
                    { text: 'CORA', link: '/docs/1.8.0/sql/function-reference#cora' },
                    { text: 'EMODnet Chemistry', link: '/docs/1.8.0/sql/function-reference#emodnet-chemistry' },
                    { text: 'SeaDataNet', link: '/docs/1.8.0/sql/function-reference#seadatanet' },
                    { text: 'Argo', link: '/docs/1.8.0/sql/function-reference#argo' },
                    { text: 'World Ocean Database', link: '/docs/1.8.0/sql/function-reference#world-ocean-database-wod' },
                  ]
                },
              ]
            },
          ]
        },
        {
          text: 'REST API',
          link: '/docs/1.8.0/api/',
          collapsed: true,
          items: [
            {
              text: 'Introduction',
              link: '/docs/1.8.0/api/',
            },
            {
              text: 'Exploring the Data Lake',
              link: '/docs/1.8.0/api/exploring-data-lake',
              collapsed: true,
              items: [
                { text: 'Datasets', link: '/docs/1.8.0/api/exploring-data-lake#datasets' },
                { text: 'Tables', link: '/docs/1.8.0/api/exploring-data-lake#tables' },
                { text: 'Functions', link: '/docs/1.8.0/api/exploring-data-lake#functions' },
                { text: 'Admin', link: '/docs/1.8.0/api/exploring-data-lake#admin' },
              ]
            },
            {
              text: 'Querying',
              link: '/docs/1.8.0/api/querying',
              collapsed: true,
              items: [
                {
                  text: 'JSON Query DSL',
                  link: '/docs/1.8.0/api/querying/json',
                  collapsed: true,
                  items: [
                    { text: 'Selecting Columns', link: '/docs/1.8.0/api/querying/json#selecting-columns' },
                    { text: 'Data Source', link: '/docs/1.8.0/api/querying/json#choosing-the-data-source-from' },
                    { text: 'Filters', link: '/docs/1.8.0/api/querying/json#filters' },
                    { text: 'Sorting & Pagination', link: '/docs/1.8.0/api/querying/json#sorting-and-pagination' },
                    { text: 'Output Formats', link: '/docs/1.8.0/api/querying/json#output-formats' },
                  ]
                },
                {
                  text: 'SQL',
                  link: '/docs/1.8.0/api/querying/sql',
                  collapsed: true,
                  items: [
                    { text: 'Query a Table', link: '/docs/1.8.0/api/querying/sql#query-a-registered-table' },
                    { text: 'Table Functions', link: '/docs/1.8.0/api/querying/sql#query-files-directly' },
                    { text: 'Output Formats', link: '/docs/1.8.0/api/querying/sql#output-formats' },
                  ]
                },
                {
                  text: 'Examples',
                  link: '/docs/1.8.0/api/querying/examples',
                },
              ]
            },
          ]
        },
        // No MCP entry here: MCP shipped after the 1.8.0 release, and this folder
        // is pinned to the v1.8.0 tag. The MCP docs live in 2.0.0 (unreleased).
        {
          text: 'Connect',
          collapsed: true,
          items: [
            {
              text: 'JetBrains DataGrip',
              link: '/docs/1.8.0/connect/jetbrains-datagrip',
            },
            {
              text: 'Beacon CLI',
              link: '/docs/1.8.0/connect/beacon-datalake-cli',
            },
            {
              text: 'Beacon TypeScript SDK',
              link: '/docs/1.8.0/connect/beacon-typescript-sdk',
            },
            {
              text: 'Admin Web UI',
              link: '/docs/1.8.0/connect/web-admin-ui',
            },
            {
              text: 'Beacon Python SDK',
              link: '/docs/1.8.0/connect/beacon-python-sdk',
            },
            {
              text: 'Python ADBC Driver',
              link: '/docs/1.8.0/connect/python-adbc',
            }
          ]
        }
      ],
      '/docs/1.7.3/': [
        {
          text: 'Introduction',
          link: '/docs/1.7.3/introduction',
        },
        {
          text: 'Getting Started',
          link: '/docs/1.7.3/getting-started',
          collapsed: false,
          items: [
            {
              text: 'Local',
              link: '/docs/1.7.3/getting-started#local',
            },
            {
              text: 'S3 / Object Storage',
              link: '/docs/1.7.3/getting-started#s3-compatible-object-storage',
            }
          ]
        },
        {
          text: 'Data Lakehouse Setup',
          link: '/docs/1.7.3/data-lake',
          collapsed: false,
          items: [
            {
              text: 'Supported Formats',
              link: '/docs/1.7.3/data-lake/datasets',
              collapsed: true,
              items: [
                {
                  text: 'Parquet',
                  link: '/docs/1.7.3/data-lake/datasets#parquet'
                },
                {
                  text: 'GeoParquet',
                  link: '/docs/1.7.3/data-lake/geoparquet'
                },
                {
                  text: 'NetCDF',
                  link: '/docs/1.7.3/data-lake/datasets#netcdf'
                },
                {
                  text: 'Zarr',
                  link: '/docs/1.7.3/data-lake/datasets#zarr'
                },
                {
                  text: 'Arrow IPC',
                  link: '/docs/1.7.3/data-lake/datasets#arrow-ipc'
                },
                {
                  text: 'ODV ASCII',
                  link: '/docs/1.7.3/data-lake/datasets#odv-ascii'
                },
                {
                  text: 'CSV',
                  link: '/docs/1.7.3/data-lake/datasets#csv'
                },
                {
                  text: 'GeoTIFF',
                  link: '/docs/1.7.3/data-lake/datasets#geotiff--cloud-optimized-geotiff'
                },
                {
                  text: 'Atlas',
                  link: '/docs/1.7.3/data-lake/datasets#atlas'
                },
                {
                  text: 'Beacon Binary Format',
                  link: '/docs/1.7.3/data-lake/datasets#beacon-binary-format-bbf'
                },
              ]
            },
            {
              text: 'SQL Tables & Views',
              collapsed: true,
              items: [
                {
                  text: 'External Tables',
                  link: '/docs/1.7.3/data-lake/external-tables',
                  collapsed: true,
                  items: [
                    { text: 'Parquet', link: '/docs/1.7.3/data-lake/external-tables#parquet' },
                    { text: 'GeoParquet', link: '/docs/1.7.3/data-lake/external-tables#geoparquet' },
                    { text: 'NetCDF', link: '/docs/1.7.3/data-lake/external-tables#netcdf' },
                    { text: 'Zarr', link: '/docs/1.7.3/data-lake/external-tables#zarr' },
                    { text: 'Atlas', link: '/docs/1.7.3/data-lake/external-tables#atlas' },
                    { text: 'CSV', link: '/docs/1.7.3/data-lake/external-tables#csv' },
                    { text: 'Arrow IPC', link: '/docs/1.7.3/data-lake/external-tables#arrow-ipc' },
                    { text: 'ODV ASCII', link: '/docs/1.7.3/data-lake/external-tables#odv-ascii' },
                    { text: 'GeoTIFF / COG', link: '/docs/1.7.3/data-lake/external-tables#geotiff-cog' },
                    { text: 'Delta Lake', link: '/docs/1.7.3/data-lake/external-tables#delta-lake' },
                  ]
                },
                {
                  text: 'Crawlers',
                  link: '/docs/1.7.3/data-lake/crawlers',
                  collapsed: true,
                  items: [
                    { text: 'CREATE CRAWLER', link: '/docs/1.7.3/data-lake/crawlers#create-crawler' },
                    { text: 'RUN / SHOW / DROP', link: '/docs/1.7.3/data-lake/crawlers#run-crawler' },
                    { text: 'Partition detection', link: '/docs/1.7.3/data-lake/crawlers#partition-detection' },
                    { text: 'Triggers', link: '/docs/1.7.3/data-lake/crawlers#triggers' },
                    { text: 'Configuration', link: '/docs/1.7.3/data-lake/crawlers#configuration' },
                    { text: 'Limitations', link: '/docs/1.7.3/data-lake/crawlers#supported-formats-and-limitations' },
                  ]
                },
                {
                  text: 'Managed Tables',
                  link: '/docs/1.7.3/sql/managed-tables',
                },
                {
                  text: 'Views',
                  link: '/docs/1.7.3/data-lake/view',
                },
                {
                  text: 'Delta Lake',
                  link: '/docs/1.7.3/data-lake/delta-lake',
                  collapsed: true,
                  items: [
                    { text: 'Defining a Delta table', link: '/docs/1.7.3/data-lake/delta-lake#defining-a-delta-external-table' },
                    { text: 'Time travel', link: '/docs/1.7.3/data-lake/delta-lake#options-time-travel' },
                    { text: 'Writing (INSERT INTO)', link: '/docs/1.7.3/data-lake/delta-lake#writing-insert-into' },
                    { text: 'Limitations', link: '/docs/1.7.3/data-lake/delta-lake#limitations' },
                  ]
                },
                {
                  text: 'Remote Tables (Federation)',
                  link: '/docs/1.7.3/data-lake/remote-tables',
                  collapsed: true,
                  items: [
                    { text: 'Defining a remote table', link: '/docs/1.7.3/data-lake/remote-tables#defining-a-remote-table' },
                    { text: 'How pushdown works', link: '/docs/1.7.3/data-lake/remote-tables#how-pushdown-works' },
                    { text: 'Schema handling', link: '/docs/1.7.3/data-lake/remote-tables#schema-handling' },
                    { text: 'Limitations', link: '/docs/1.7.3/data-lake/remote-tables#limitations' },
                  ]
                },
                {
                  text: 'SQL Databases (Postgres / MySQL)',
                  link: '/docs/1.7.3/data-lake/sql-databases',
                  collapsed: true,
                  items: [
                    { text: 'Credentials', link: '/docs/1.7.3/data-lake/sql-databases#credentials' },
                    { text: 'Defining a database table', link: '/docs/1.7.3/data-lake/sql-databases#defining-a-sql-database-table' },
                    { text: 'How pushdown works', link: '/docs/1.7.3/data-lake/sql-databases#how-pushdown-works' },
                    { text: 'Limitations', link: '/docs/1.7.3/data-lake/sql-databases#limitations' },
                  ]
                }
              ]
            },
            {
              text: 'Configuration',
              link: '/docs/1.7.3/data-lake/configuration',
            },
            {
              text: 'Performance Tuning',
              link: '/docs/1.7.3/data-lake/performance-tuning',
              collapsed: true,
              items: [
                {
                  text: 'Settings',
                  link: '/docs/1.7.3/data-lake/performance-tuning#beacon-query-engine-settings'
                },
                {
                  text: 'NetCDF',
                  link: '/docs/1.7.3/data-lake/performance-tuning#netcdf-tuning'
                },
                {
                  text: 'Zarr',
                  link: '/docs/1.7.3/data-lake/performance-tuning#zarr-predicate-pushdown'
                },
                {
                  text: 'Atlas',
                  link: '/docs/1.7.3/data-lake/performance-tuning#atlas-tuning'
                }
              ]
            }
          ]
        },
        {
          text: 'SQL Guide',
          link: '/docs/1.7.3/sql/',
          collapsed: true,
          items: [
            {
              text: 'Introduction',
              link: '/docs/1.7.3/sql/',
            },
            {
              text: 'Querying',
              collapsed: true,
              items: [
                {
                  text: 'SELECT',
                  link: '/docs/1.7.3/sql/select',
                },
                {
                  text: 'WHERE',
                  link: '/docs/1.7.3/sql/where',
                  collapsed: true,
                  items: [
                    { text: 'Comparison', link: '/docs/1.7.3/sql/where#comparison-operators' },
                    { text: 'BETWEEN', link: '/docs/1.7.3/sql/where#between' },
                    { text: 'IN', link: '/docs/1.7.3/sql/where#in' },
                    { text: 'LIKE', link: '/docs/1.7.3/sql/where#like' },
                    { text: 'IS NULL', link: '/docs/1.7.3/sql/where#is-null--is-not-null' },
                    { text: 'AND / OR / NOT', link: '/docs/1.7.3/sql/where#and--or--not' },
                    { text: 'Date & time', link: '/docs/1.7.3/sql/where#date-and-time-filtering' },
                  ]
                },
                {
                  text: 'GROUP BY',
                  link: '/docs/1.7.3/sql/group-by',
                },
                {
                  text: 'JOIN',
                  link: '/docs/1.7.3/sql/join',
                  collapsed: true,
                  items: [
                    { text: 'INNER JOIN', link: '/docs/1.7.3/sql/join#inner-join' },
                    { text: 'LEFT JOIN', link: '/docs/1.7.3/sql/join#left-join' },
                  ]
                },
                {
                  text: 'Reading Files',
                  link: '/docs/1.7.3/sql/table-functions',
                  collapsed: true,
                  items: [
                    { text: 'read_netcdf', link: '/docs/1.7.3/sql/table-functions#read_netcdf' },
                    { text: 'read_zarr', link: '/docs/1.7.3/sql/table-functions#read_zarr' },
                    { text: 'read_atlas', link: '/docs/1.7.3/sql/table-functions#read_atlas' },
                    { text: 'read_parquet', link: '/docs/1.7.3/sql/table-functions#read_parquet' },
                    { text: 'read_arrow', link: '/docs/1.7.3/sql/table-functions#read_arrow' },
                    { text: 'read_csv', link: '/docs/1.7.3/sql/table-functions#read_csv' },
                    { text: 'read_odv_ascii', link: '/docs/1.7.3/sql/table-functions#read_odv_ascii' },
                    { text: 'read_bbf', link: '/docs/1.7.3/sql/table-functions#read_bbf' },
                    { text: 'read_tiff', link: '/docs/1.7.3/sql/table-functions#read_tiff' },
                    { text: 'read_delta', link: '/docs/1.7.3/sql/table-functions#read_delta' },
                  ]
                },
                {
                  text: 'UNION ALL BY NAME',
                  link: '/docs/1.7.3/sql/union-by-name',
                },
                {
                  text: 'GeoParquet',
                  link: '/docs/1.7.3/sql/geoparquet',
                },
                {
                  text: 'Remote Tables',
                  link: '/docs/1.7.3/sql/remote-tables',
                },
              ]
            },
            {
              text: 'DDL',
              collapsed: true,
              items: [
                {
                  text: 'CREATE EXTERNAL TABLE',
                  link: '/docs/1.7.3/sql/create-table',
                  collapsed: true,
                  items: [
                    { text: 'IF NOT EXISTS', link: '/docs/1.7.3/sql/create-table#if-not-exists' },
                    { text: 'OR REPLACE', link: '/docs/1.7.3/sql/create-table#or-replace' },
                    { text: 'PARTITIONED BY', link: '/docs/1.7.3/sql/create-table#partitioned-by' },
                    { text: 'DROP TABLE', link: '/docs/1.7.3/sql/create-table#drop-table' },
                  ]
                },
                {
                  text: 'CREATE TABLE (Managed)',
                  link: '/docs/1.7.3/sql/managed-tables',
                  collapsed: true,
                  items: [
                    { text: 'Storage engine', link: '/docs/1.7.3/sql/managed-tables#choosing-the-storage-engine' },
                    { text: 'CREATE TABLE AS SELECT', link: '/docs/1.7.3/sql/managed-tables#create-table-as-select' },
                    { text: 'INSERT INTO', link: '/docs/1.7.3/sql/managed-tables#insert-into' },
                    { text: 'DELETE', link: '/docs/1.7.3/sql/managed-tables#delete' },
                    { text: 'UPDATE', link: '/docs/1.7.3/sql/managed-tables#update' },
                    { text: 'ALTER TABLE', link: '/docs/1.7.3/sql/managed-tables#alter-table' },
                    { text: 'Indexes', link: '/docs/1.7.3/sql/managed-tables#indexes' },
                    { text: 'DROP TABLE', link: '/docs/1.7.3/sql/managed-tables#drop-table' },
                  ]
                },
                {
                  text: 'CREATE VIEW',
                  link: '/docs/1.7.3/sql/create-view',
                },
                {
                  text: 'CREATE MATERIALIZED VIEW',
                  link: '/docs/1.7.3/sql/create-materialized-view',
                  collapsed: true,
                  items: [
                    { text: 'Querying', link: '/docs/1.7.3/sql/create-materialized-view#querying' },
                    { text: 'REFRESH', link: '/docs/1.7.3/sql/create-materialized-view#refresh' },
                    { text: 'DROP', link: '/docs/1.7.3/sql/create-materialized-view#drop' },
                  ]
                },
                {
                  text: 'CREATE CRAWLER',
                  link: '/docs/1.7.3/data-lake/crawlers',
                  collapsed: true,
                  items: [
                    { text: 'Options', link: '/docs/1.7.3/data-lake/crawlers#options' },
                    { text: 'RUN CRAWLER', link: '/docs/1.7.3/data-lake/crawlers#run-crawler' },
                    { text: 'SHOW CRAWLERS', link: '/docs/1.7.3/data-lake/crawlers#show-crawlers' },
                    { text: 'DROP CRAWLER', link: '/docs/1.7.3/data-lake/crawlers#drop-crawler' },
                  ]
                },
              ]
            },
            {
              text: 'Introspection',
              link: '/docs/1.7.3/sql/table-functions-utility',
              collapsed: true,
              items: [
                { text: 'read_schema', link: '/docs/1.7.3/sql/table-functions-utility#read_schema' },
                { text: 'list_datasets', link: '/docs/1.7.3/sql/table-functions-utility#list_datasets' },
                { text: 'view_dataset_statistics', link: '/docs/1.7.3/sql/table-functions-utility#view_dataset_statistics' },
                { text: 'view_external_table_statistics', link: '/docs/1.7.3/sql/table-functions-utility#view_external_table_statistics' },
                { text: 'view_statistics_cache', link: '/docs/1.7.3/sql/table-functions-utility#view_statistics_cache' },
              ]
            },
            {
              text: 'Function Reference',
              link: '/docs/1.7.3/sql/function-reference',
              collapsed: true,
              items: [
                { text: 'Aggregate', link: '/docs/1.7.3/sql/function-reference#aggregate-functions' },
                { text: 'Math', link: '/docs/1.7.3/sql/function-reference#math-functions' },
                { text: 'String', link: '/docs/1.7.3/sql/function-reference#string-functions' },
                { text: 'Regular Expressions', link: '/docs/1.7.3/sql/function-reference#regular-expression-functions' },
                { text: 'Binary String', link: '/docs/1.7.3/sql/function-reference#binary-string-functions' },
                { text: 'Date & Time', link: '/docs/1.7.3/sql/function-reference#date-and-time-functions' },
                { text: 'Conditional', link: '/docs/1.7.3/sql/function-reference#conditional-expressions' },
                { text: 'Casting', link: '/docs/1.7.3/sql/function-reference#casting' },
                {
                  text: 'Beacon-specific',
                  link: '/docs/1.7.3/sql/function-reference#beacon-specific-functions',
                  collapsed: true,
                  items: [
                    { text: 'beacon_version', link: '/docs/1.7.3/sql/function-reference#beacon_version' },
                    { text: 'coalesce_label', link: '/docs/1.7.3/sql/function-reference#coalesce_label' },
                    { text: 'cast_int8_as_char', link: '/docs/1.7.3/sql/function-reference#cast_int8_as_charn' },
                    { text: 'try_arrow_cast', link: '/docs/1.7.3/sql/function-reference#try_arrow_castexpr-type_str' },
                  ]
                },
                {
                  text: 'Geospatial',
                  link: '/docs/1.7.3/sql/function-reference#geospatial-functions',
                  collapsed: true,
                  items: [
                    { text: 'st_within_point', link: '/docs/1.7.3/sql/function-reference#st_within_point' },
                    { text: 'st_geojson_as_wkt', link: '/docs/1.7.3/sql/function-reference#st_geojson_as_wkt' },
                  ]
                },
                {
                  text: 'Domain Mapping',
                  link: '/docs/1.7.3/sql/function-reference#domain-mapping-functions',
                  collapsed: true,
                  items: [
                    { text: 'pressure_to_depth_teos_10', link: '/docs/1.7.3/sql/function-reference#pressure_to_depth_teos_10' },
                    { text: 'map_units', link: '/docs/1.7.3/sql/function-reference#map_units' },
                    { text: 'Common', link: '/docs/1.7.3/sql/function-reference#common' },
                    { text: 'CMEMS', link: '/docs/1.7.3/sql/function-reference#cmems' },
                    { text: 'CORA', link: '/docs/1.7.3/sql/function-reference#cora' },
                    { text: 'EMODnet Chemistry', link: '/docs/1.7.3/sql/function-reference#emodnet-chemistry' },
                    { text: 'SeaDataNet', link: '/docs/1.7.3/sql/function-reference#seadatanet' },
                    { text: 'Argo', link: '/docs/1.7.3/sql/function-reference#argo' },
                    { text: 'World Ocean Database', link: '/docs/1.7.3/sql/function-reference#world-ocean-database-wod' },
                  ]
                },
              ]
            },
          ]
        },
        {
          text: 'REST API',
          link: '/docs/1.7.3/api/',
          collapsed: true,
          items: [
            {
              text: 'Introduction',
              link: '/docs/1.7.3/api/',
            },
            {
              text: 'Exploring the Data Lake',
              link: '/docs/1.7.3/api/exploring-data-lake',
              collapsed: true,
              items: [
                { text: 'Datasets', link: '/docs/1.7.3/api/exploring-data-lake#datasets' },
                { text: 'Tables', link: '/docs/1.7.3/api/exploring-data-lake#tables' },
                { text: 'Functions', link: '/docs/1.7.3/api/exploring-data-lake#functions' },
                { text: 'Admin', link: '/docs/1.7.3/api/exploring-data-lake#admin' },
              ]
            },
            {
              text: 'Querying',
              link: '/docs/1.7.3/api/querying',
              collapsed: true,
              items: [
                {
                  text: 'JSON Query DSL',
                  link: '/docs/1.7.3/api/querying/json',
                  collapsed: true,
                  items: [
                    { text: 'Selecting Columns', link: '/docs/1.7.3/api/querying/json#selecting-columns' },
                    { text: 'Data Source', link: '/docs/1.7.3/api/querying/json#choosing-the-data-source-from' },
                    { text: 'Filters', link: '/docs/1.7.3/api/querying/json#filters' },
                    { text: 'Sorting & Pagination', link: '/docs/1.7.3/api/querying/json#sorting-and-pagination' },
                    { text: 'Output Formats', link: '/docs/1.7.3/api/querying/json#output-formats' },
                  ]
                },
                {
                  text: 'SQL',
                  link: '/docs/1.7.3/api/querying/sql',
                  collapsed: true,
                  items: [
                    { text: 'Query a Table', link: '/docs/1.7.3/api/querying/sql#query-a-registered-table' },
                    { text: 'Table Functions', link: '/docs/1.7.3/api/querying/sql#query-files-directly' },
                    { text: 'Output Formats', link: '/docs/1.7.3/api/querying/sql#output-formats' },
                  ]
                },
                {
                  text: 'Examples',
                  link: '/docs/1.7.3/api/querying/examples',
                },
              ]
            },
          ]
        },
        {
          text: 'Connect',
          collapsed: true,
          items: [
            {
              text: 'JetBrains DataGrip',
              link: '/docs/1.7.3/connect/jetbrains-datagrip',
            },
            {
              text: 'Beacon Datalake CLI',
              link: '/docs/1.7.3/connect/beacon-datalake-cli',
            },
            {
              text: 'Beacon Python SDK',
              link: '/docs/1.7.3/connect/beacon-python-sdk',
            },
            {
              text: 'Python ADBC Driver',
              link: '/docs/1.7.3/connect/python-adbc',
            }
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
            collapsed: true,
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
            collapsed: true,
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
                    collapsed: true,
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
            collapsed: true,
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
            collapsed: true,
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
              collapsed: true,
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
              collapsed: true,
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
              collapsed: true,
              items: [

              ]
            },
            {
              text: 'Querying API',
              link: '/docs/1.5.0/query-docs/querying/introduction',
              collapsed: true,
              items: [
                {
                  text: 'Introduction',
                  link: '/docs/1.5.0/query-docs/querying/introduction'
                },
                {
                  text: 'Querying with SQL',
                  link: '/docs/1.5.0/query-docs/querying/sql',
                  collapsed: true,
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
                  collapsed: true,
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
              collapsed: true,
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
              text: 'What\'s new in 1.8.0',
              link: '/docs/changelog/release-1.8.0'
            },
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
              text: '1.8.0',
              link: '/docs/changelog'
            },
            {
              text: '1.7.3',
              link: '/docs/changelog'
            },
            {
              text: '1.7.2',
              link: '/docs/changelog'
            },
            {
              text: '1.7.1',
              link: '/docs/changelog'
            },
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
    ],
    editLink: {
      pattern: 'https://github.com/maris-development/beacon/edit/main/docs/:path',
      text: 'Edit this page on GitHub'
    },
    footer: {
      message: 'Released under the AGPL-3.0 License.',
      copyright: 'Copyright © MARIS & the Beacon contributors'
    }
  }
})
