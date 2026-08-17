### What changes does this PR make?
<!--
Describe the change in one or two sentences. Write it in the simple present tense.
Example: "Read Delta Lake tables through a new external table type."
-->

### Why do you make these changes?
<!--
Give the problem, the bug, or the user need. Link the related issue.
Example: "Closes #123." or "Fixes #123."
-->

### How do you make these changes?
<!--
Describe the approach. Name the new crates, modules, or public APIs.
Mention the alternatives that you rejected, and the reason.
-->

### Does this PR change a public interface or a configuration?
<!--
List the changed SQL syntax, HTTP routes, environment variables, or client APIs.
Mark each breaking change with "BREAKING:". If no, write "No".
-->

### How do you test these changes?
<!--
List the new or the changed tests. Give the commands that you run.
Example:
  cargo clippy --workspace --lib --bins --tests
  cargo test --workspace --no-fail-fast --lib --bins --tests
  cargo fmt --all --check
Add screenshots for a UI change.
-->

### Checklist
- [ ] The title of the PR describes the change, and it ends with the issue number if one exists.
- [ ] The code follows the style of the project, and `cargo fmt --all --check` passes.
- [ ] `cargo clippy --workspace --lib --bins --tests` reports no new warnings.
- [ ] `cargo test --workspace --lib --bins --tests` passes.
- [ ] The new code has tests, or the PR explains why it does not.
- [ ] The documentation, the README, and the CHANGELOG show the change.
- [ ] The PR contains no unrelated changes.

### Was this patch authored or co-authored using generative AI tooling?
<!--
If generative AI tooling has assisted in authoring or reviewing this patch, please include
an 'Assisted-by: ' trailer that identifies the agent and model.
For example: 'Assisted-by: Claude:claude-opus-5' or 'Assisted-by: Claude Opus 5'.
If no, write 'No'.
Please refer to the [ASF Generative Tooling Guidance](https://www.apache.org/legal/generative-tooling.html) for details.
-->
