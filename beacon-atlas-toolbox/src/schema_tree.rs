use std::collections::BTreeMap;

#[derive(Debug, Default)]
struct SchemaNode {
    data_type: Option<String>,
    children: BTreeMap<String, SchemaNode>,
}

fn insert_field_path(root: &mut SchemaNode, field_name: &str, data_type: String) {
    let mut current = root;
    for segment in field_name.split('.') {
        current = current.children.entry(segment.to_string()).or_default();
    }
    current.data_type = Some(data_type);
}

fn format_schema_node(
    name: &str,
    node: &SchemaNode,
    prefix: &str,
    is_last: bool,
    is_attribute: bool,
    out: &mut Vec<String>,
) {
    let connector = if is_last { "`-- " } else { "|-- " };
    let attribute_tag = if is_attribute { " [attribute]" } else { "" };

    if let Some(data_type) = &node.data_type {
        out.push(format!(
            "{prefix}{connector}{name}: {data_type}{attribute_tag}"
        ));
    } else {
        out.push(format!("{prefix}{connector}{name}{attribute_tag}"));
    }

    let child_prefix = format!("{prefix}{}", if is_last { "    " } else { "|   " });
    let mut ordered_children = node.children.iter().collect::<Vec<_>>();
    ordered_children.sort_by(|(left_name, _), (right_name, _)| {
        left_name
            .to_ascii_lowercase()
            .cmp(&right_name.to_ascii_lowercase())
            .then_with(|| left_name.cmp(right_name))
    });

    let child_count = ordered_children.len();
    for (idx, (child_name, child)) in ordered_children.into_iter().enumerate() {
        format_schema_node(
            child_name,
            child,
            &child_prefix,
            idx + 1 == child_count,
            true,
            out,
        );
    }
}

/// Render a schema with dotted names grouped hierarchically.
///
/// Dotted descendants are shown as `[attribute]`, and every level is sorted
/// alphabetically by field name.
pub(crate) fn render_schema_tree(schema: &arrow::datatypes::Schema) -> String {
    let mut root = SchemaNode::default();
    for field in schema.fields() {
        insert_field_path(&mut root, field.name(), field.data_type().to_string());
    }

    let mut lines = Vec::new();
    let mut ordered_roots = root.children.iter().collect::<Vec<_>>();
    ordered_roots.sort_by(|(left_name, _), (right_name, _)| {
        left_name
            .to_ascii_lowercase()
            .cmp(&right_name.to_ascii_lowercase())
            .then_with(|| left_name.cmp(right_name))
    });

    let root_count = ordered_roots.len();
    for (idx, (name, node)) in ordered_roots.into_iter().enumerate() {
        format_schema_node(name, node, "", idx + 1 == root_count, false, &mut lines);
    }

    lines.join("\n")
}
