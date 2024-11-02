# DBT Project Setup Guide for `is459_redshift_project`

## Prerequisites

- **Python Environment**: Set up a `conda` environment with Python 3.10:

  ```bash
  conda create -n dbt_env python=3.10
  conda activate dbt_env
  ```

- **dbt and Dependencies**: Install `dbt-core`, `dbt-redshift`, and any other dependencies from `requirements.txt`:

  First, ensure your `requirements.txt` includes `dbt-core` and `dbt-redshift` (and any other required packages):
  
  ```
  dbt-core
  dbt-redshift
  ```

  Then, install dependencies:

  ```bash
  pip install -r requirements.txt
  ```

- **Access to AWS Redshift**: Ensure your Redshift credentials have the necessary permissions for creating tables and querying data within your assigned schema.

## Step 1: Setting Up the dbt Profile

The `dbt` profile defines connection details for your Redshift instance. The configuration is stored in `~/.dbt/profiles.yml`. Here’s an example configuration:

### Example `profiles.yml` (for Redshift)

```yaml
is459-redshift-db:
  target: dev
  outputs:
    dev:
      type: redshift
      host: default-workgroup.820242926303.us-east-1.redshift-serverless.amazonaws.com
      user: flights_user
      password: <password>
      port: 5439
      dbname: dev
      schema: flight_data
      connect_timeout: 60
```

Replace the following placeholders:
- **host**: The Redshift cluster endpoint.
- **user**: Your Redshift username.
- **password**: Your Redshift password.
- **port**: The Redshift port (5439 by default).
- **dbname**: The database name.
- **schema**: The schema you want to use for this dbt project.

### Testing the Connection

Run the following command to test the connection:

```bash
dbt debug --profile is459-redshift-db
```

If successful, `dbt` will confirm a connection to Redshift.

---

## Step 2: Configuring the dbt Project File

The `dbt_project.yml` file defines project settings and where to store models. Here’s an example configuration for `is459_redshift_project`:

### Example `dbt_project.yml`

```yaml
name: is459_redshift_project
version: 1.0
profile: is459-redshift-db

model-paths: ["models"]

models:
  is459_redshift_project:
    intermediate:
      +enabled: true
      kaggle:
        +enabled: true
```

This configuration:
- Points to `is459-redshift-db` in `profiles.yml`.
- Enables models in `models/intermediate/kaggle/`.

---

## Step 3: Adding New Models

### Creating Model Files

1. Navigate to the `models/intermediate/kaggle/` directory.
2. Add new model files (e.g., `my_new_model.sql`) in this directory.

### Example SQL Model (`my_new_model.sql`)

Here’s a basic example of a SQL model that counts rows in a source table:

```sql
{{ config(
    materialized='table'
) }}

SELECT COUNT(*) AS row_count
FROM {{ source('kaggle', 'flights') }}
```

### Defining Sources

Ensure that sources are defined in a `schema.yml` file, like so:

```yaml
version: 2

sources:
  - name: kaggle
    database: dev
    schema: kaggle
    tables:
      - name: flights
```

Place `schema.yml` in the `models` directory. This configuration allows `dbt` to reference `{{ source('kaggle', 'flights') }}` in models.

---

## Step 4: Running Models

Once everything is configured, you can run dbt models with the following commands:

- **Run all models**:

  ```bash
  dbt run
  ```

- **Run specific models**:

  ```bash
  dbt run --models intermediate.kaggle.*
  ```

- **Run with full refresh** (useful for table materializations):

  ```bash
  dbt run --models intermediate.kaggle.* --full-refresh
  ```

### Other Commands

- **View logs in verbose mode** (helpful for debugging):

  ```bash
  dbt run --models intermediate.kaggle.* -v
  ```
