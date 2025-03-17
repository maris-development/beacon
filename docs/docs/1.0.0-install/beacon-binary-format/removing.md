# Removing Datasets From Beacon Binary Format

The Beacon API allows you to remove datasets from Beacon Binary Format files. The datasets can be removed using the Beacon Swagger Interface or API calls.

Removing datasets are done using in place deletions vectors. This means that deletions are O(1) and do not require rewriting the entire file (only a single memory page). This makes it easy to update and manage datasets without the need to re-import the data.

## Example Removing Dataset

```http
DELETE /api/admin/bbf/remove-dataset?bbf_path=bbf_filename&remove_name=remove_dataset_name HTTP/1.1
Authorization: Basic <base64-encoded-username-and-password>
```
