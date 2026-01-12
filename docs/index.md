---
# https://vitepress.dev/reference/default-theme-home-page
layout: home

hero:
  name: "Beacon"
  text: "Open Source High-Performance ARCO Data Lake Engine"
  tagline: Making climate data available to everyone
  actions:
    - theme: brand
      text: Install your own node
      link: /docs/1.5.0-install
    - theme: brand
      text: How to query
      link: /docs/1.5.0/query-docs/
    - theme: alt
      text: Existing nodes
      link: /available-nodes/available-nodes

features:
  - title: Performance 
    details: Beacon makes use of the power of Rust🦀 together with Arrow and Apache Datafusion to provide unmatched performance. Experience lightning-fast ⚡ data retrieval, enabling you to explore and query millions datasets on the fly with ease. With Beacon, you'll never be held back by sluggish data retrieval again.
    icon: 🚀
  - title: Open Source
    details: Beacon is fully open source available under the AGPL V3 license👐. This means you have full control over your data and can contribute to its development.
    icon: 🤝
  - title: Efficiency
    details: Say goodbye to storage woes! 📊 Store and manage millions of datasets efficiently, in the cloud using S3 or locally. Beacon ensures you will have high-performance access to your datasets.
    icon: ⚙️
  - title: It just works
    details: Beacon has been designed for simplicity! 🌟 Experience seamless integration with existing file formats such as NetCDF, Zarr, Parquet and many others. 🛠️ You'll have a powerful data lake solution up and running with all of your datasets within minutes, ready to support your data exploration.
    icon: 🙂

---
