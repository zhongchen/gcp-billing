resource "google_bigquery_dataset" "billing-dataset" {
  dataset_id = "gcp_billing"
}

resource "google_bigquery_table" "gcp-billing-taxonomy" {
  dataset_id = google_bigquery_dataset.billing-dataset.dataset_id
  table_id = "gcp_billing_taxonomy"
  schema = file("schema/gcp_billing_taxonomy.json")
}

resource "google_bigquery_table" "gcp-billing-taxonomy-with-category" {
  dataset_id = google_bigquery_dataset.billing-dataset.dataset_id
  table_id = "gcp_billing_taxonomy_with_category"
  schema = file("schema/gcp_billing_taxonomy_with_category.json")
}
