---
# https://vitepress.dev/reference/default-theme-home-page
layout: home

hero:
  name: "Beacon"
  text: "A High-Performance ARCO Data Lake Solution"
  tagline: Making climate data available to everyone
  actions:
    - theme: brand
      text: Install your own node
      link: /docs/1.3.0-install
    - theme: brand
      text: How to query
      link: /docs/1.3.0/query-docs/
    - theme: alt
      text: Existing nodes
      link: /available-nodes/available-nodes

features:
  - title: Performance 
    details: Beacon makes use of the power of Rust🦀 together with Arrow and Apache Datafusion to provide unmatched performance. Experience lightning-fast ⚡ data retrieval, enabling you to explore and query millions datasets on the fly with ease. With Beacon, you'll never be held back by sluggish data retrieval again.
    icon: 🚀
  - title: Efficiency
    details: Say goodbye to storage woes! 📊 Store and manage millions of datasets efficiently, in the cloud using S3 or locally. Beacon ensures you will have high-performance access to your datasets.
    icon: ⚙️
  - title: It just works
    details: Beacon has been designed for simplicity! 🌟 With an intuitive Rest API and seamless integration with existing file formats such as NetCDF, Zarr, Parquet and many others, setting up Beacon is straightforward. 🛠️ Because Beacon can understand every possible dimensionality of your data, you'll have a powerful data lake solution up and running with all of your datasets within minutes, ready to support your data exploration.
    icon: 🙂

---
