from google.cloud import bigquery
import os


def compute_category(service, sku, taxonomy):
    if service == 'Compute Engine':
        if taxonomy is None:
            return ""

        if (sku.startswith('Network') or
                taxonomy.startswith('GCP > Network')):
            return 'Network'

        if sku.startswith('Commitment') or taxonomy.startswith('GCP > Compute > GPU') \
                or taxonomy.startswith('GCP > Compute > Persistent Disk') \
                or taxonomy.startswith('GCP > Compute > GCE'):
            return 'Compute Engine'

        if taxonomy.startswith('GCP > Compute > Persistent Disk'):
            return 'Persistent Disk'

        if taxonomy.startswith('GCP > Compute > Persistent Disk > Standard > Capacity') \
                or taxonomy.startswith('GCP > Compute > Persistent Disk > SSD > Capacity'):
            return 'Persistent Disk Capacity'

    if service.startswith('BigQuery'):
        return 'BigQuery'

    if service.startswith('Cloud Storage'):
        return 'Cloud Storage'

    if service == 'Compute Engine' and sku.find('Core') != -1:
        return 'Compute Cores'

    return ""


def compute_type(service, sku, taxonomy):
    if service == 'Compute Engine':
        if taxonomy is None:
            return ""

        if sku.startswith('Commitment'):
            return 'Commitment'

        if taxonomy.startswith('GCP > Compute > Persistent Disk > Standard > Capacity'):
            return 'HDD Capacity'

        if taxonomy.startswith('GCP > Compute > Persistent Disk > SSD > Capacity'):
            return 'SSD Capacity'

        if taxonomy.startswith('GCP > Compute > GPU'):
            return 'GPUs'

        if taxonomy.startswith('GCP > Compute > Persistent Disk'):
            if sku.find('PD') != -1:
                return 'Standard Disk'
            if sku.find('SSD') != -1:
                return 'SSD Disk'

        if taxonomy.startswith('GCP > Compute > GCE'):
            index = sku.rfind(' running')
            if index != -1:
                return sku[0: index]

            return sku

    if service == 'BigQuery':
        if sku.startswith('Active Storage'):
            return 'Active Storage'

        if sku.startswith('Analysis'):
            return 'Analysis'

        if sku.startswith('Long Term Storage'):
            return 'Long Term Storage'

        if sku.startswith('Streaming Insert'):
            return 'Streaming Insert'

    if service == 'Cloud Storage':
        if taxonomy.startswith('GCP > Network > Egress > GCS > Premium') \
                or taxonomy.startswith('GCP > Network > Egress > GAE > Premium'):
            return 'Egress Outside GCP'

        if taxonomy.startswith('GCP > Storage > GCS > Ops'):
            return 'API Operations'

        if taxonomy.startswith('GCP > Network > Cloud CDN '):
            return 'Cloud CDN'

        if taxonomy.startswith('GCP > Network > Egress > GCS > Inter-region'):
            return 'Egress Inter-region'

        if taxonomy.startswith('GCP > Network > Interconnect'):
            return 'Egress Interconnect'

        if taxonomy.startswith('GCP > Storage > GCS > Storage > Standard') \
                or sku.startswith('Standard Storage'):
            return 'Standard Storage'

        if taxonomy.startswith('GCP > Storage > GCS > Storage > Nearline') \
                or sku.startswith('Nearline Storage'):
            return 'Nearline Storage'

        if taxonomy.startswith('GCP > Storage > GCS > Storage > Standard') \
                or sku.startswith('Standard Storage'):
            return 'Standard Storage'

        if taxonomy.startswith('GCP > Storage > GCS > Storage > DRA') \
                or sku.startswith('Durable Reduced Availability'):
            return 'Durable Reduced Availability Storage'

        if taxonomy.startswith('GCP > Storage > GCS > Storage > Coldline') \
                or sku.startswith('Coldline'):
            return 'Coldline Storage'

        if taxonomy.startswith('GCP > Storage > GCS > Storage > Archive') \
                or sku.startswith('Archive'):
            return 'Archive Storage'

    return ""


if __name__ == '__main__':
    project = os.getenv("PROJECT_ID", 'achintapatla-project')
    client = bigquery.Client()

    query = """
        SELECT * FROM `{project}.gcp_billing.gcp_pricing_taxonomy`
        """.format(project=project)

    query_job = client.query(query)
    rows = query_job.result()

    destination_table_name = '{project}.gcp_billing.gcp_pricing_taxonomy_with_category'.format(
        project=project)

    clear_table_statement = """
    CREATE OR REPLACE TABLE {table_name}
    AS SELECT * FROM {table_name} LIMIT 0;
    """.format(table_name=destination_table_name)

    query_job = client.query(clear_table_statement)
    # wait for the job to complete
    query_job.result()

    destination_table = client.get_table(destination_table_name)
    output_rows = []
    for row in rows:
        service = row.get("service_description")
        sku = row.get("sku_description")
        taxonomy = row.get("product_taxonomy")
        category = compute_category(service, sku, taxonomy)
        tpe = compute_type(service, sku, taxonomy)
        output_row = (
            row.get("google_service"),
            service,
            sku,
            taxonomy,
            row.get("unit_description"),
            category,
            tpe,
            "",  # subtype
            row.get("price_reason"),
            row.get("discount"),
            row.get("service_id"),
            row.get("sku_id"),
            row.get("per_unit_quantity"),
            row.get("tiered_usage"),
            row.get("list_price_in_dollars"),
            row.get("contract_price_in_dollars"),
            row.get("effective_discount"),
        )
        output_rows.append(output_row)

    print("Insert {} records into the destination table".format(len(output_rows)))
    errors = client.insert_rows(destination_table, output_rows)
    if not errors:
        print("New rows have been added")
