# brs-dbt

## Environment Setup
### Install and Update dbt

1. Install [Homebrew](https://brew.sh/) and run:
    ```
    brew update
    brew install git
    brew tap fishtown-analytics/dbt
    brew install dbt
    ```
2. Test installation 
    ```
    dbt --version
    ```
3. Upgrading
    ```
    brew update
    brew upgrade dbt
    ```

### Connect to BigQuery

1. Create a file in the `~/.dbt/` directory named `profiles.yml`.
2. Copy the following into the file (make sure to update dataset name):
    ```
    brs:
      outputs:
        dev:
          type: bigquery
          method: oauth
          project: brs-clouddata-prod
          dataset: dbt_(your name)
          threads: 5
          timeout_seconds: 300
          location: US
          priority: interactive
          maximum_bytes_billed: 1000000000000
      target: dev
    ```
3. To connect to BigQuery using the `oauth` method, follow these steps:
    
    a. Make sure the gcloud command is [installed on your computer](https://cloud.google.com/sdk/docs/install)

    b. Activate the application-default account with
    ```
    gcloud auth application-default login 
    ```

    A browser window should open, and you should be promoted to log into your Google account. Once you've done that, dbt will use your oauth'd credentials to connect to BigQuery.

4. Execute the debug command from your project to confirm that you can successfully connect
    ```
    dbt debug
    ```

    Confirm that the last line of the output is `Connection test: OK connection ok`


## Running DBT

1. Remember to change into the `brs` directory from the command line before running models

2. If there is a `CSV`  referenced in the model, use:
    ```
    dbt seed
    ```
3. Execute the `run` command to build models:
    ```
    dbt run # This runs all models
    dbt run [-m *] # This runs a specific model
    dbt run [-m +*] # This runs the indicated model and models from upstream
    dbt run [-m *+] # This runs the indicated model and models from downstream
    dbt run [-m +*+] # This runs the indicated model and all models from upstream and downstream
    ```

4. Use `--full-refresh` argument to `dbt run` when the schema of an incremental model changes and you need to recreate it, or you want to reprocess the entirety of the incremental model because of new logic in the model code
    ```
    dbt run --full-refresh 
    dbt run --full-refresh [-m *]
    dbt run --full-refresh [-m +*]
    dbt run --full-refresh [-m *+]
    dbt run --full-refresh [-m +*+]
    ```
5. Test the output of the models:
    ```
    dbt test
    dbt test [-m *]
    ```
6. Generate documentation for the project:
    ```
    dbt docs generate
    ```
7. View the documentation for the project:
    ```
    dbt docs serve
    ```
8. Compile and generate executable SQL from source `model`, `test`, and `analysis` files (find these compiled SQL files in the target/ directory)
    ```
    dbt compile (-m model)
    ```

9.  After you make some model changes or create new models, etc. Run these commands to ensure everything is working as expected.
    ```
    dbt source snapshot-freshness # (all should pass) 
    dbt run --full-refresh [-m *] # (all should succeeded) 
    dbt run [-m *] # (runs incrementally) 
    dbt test [-m *] # (all should pass)
    ```

## DBT Beyond
### Sources

1. Declaring a source \
    Sources are defined in .yml files in the `models` directory , nested under a sources: key.
2. Selecting from a source \
    Once a source has been defined, it can be referenced from a model using the {{ source() }} function. `e.g.{{ source('stage', 'tdc__brs__ORDER_LINE_ITEM') }}`
    ```
    SELECT  AGENCY_ID AS agency_id
          , CREATED_DATE AS created_ts
          , LAST_UPDATED_DATE AS last_updated_ts
          , AGENCY_CODE AS agency_code
          , DESCRIPTION AS agency_description
          , IF(ACTIVE = 1, TRUE, FALSE) AS is_active
            FROM  {{ source('stage', 'tdc__brs__AGENCY') }}

3. Testing and documenting sources 
```
sources:
  - name: stage
    description: stage dataset description
    tables:
      - name: tdc__brs__ORDER_LINE_ITEM
        description: >
          This table contains line item level data from transaction orders
        columns:
          - name: order_line_item_id
            description: Primary key of the order_line_item table
            tests:
              - unique
              - not_null
          - name: filed_2
            description: field_2 description

  - name: wheelhouse_red_sox --dataset
    database: mlb-dataeng-prod --project in BQ
    tables:
       - name: forms2_detail
```
4. Snapshotting source data freshness \
With a couple of extra configs, dbt can optionally snapshot the "freshness" of the data in your source tables. This is useful for understanding if your data pipelines are in a healthy state 

a. Declaring source freshness (example from https://docs.getdbt.com/docs/building-a-dbt-project/using-sources) \
To configure sources to snapshot freshness information, add a freshness block to your source and loaded_at_field to your table declaration
```
sources:
  - name: jaffle_shop
    database: raw
    freshness: # default freshness
      warn_after: {count: 12, period: hour}
      error_after: {count: 24, period: hour}
    loaded_at_field: _etl_loaded_at
```
    
b. snapshot command

```
dbt source snapshot-freshness
```
Behind the scenes, dbt uses the freshness properties to construct a select query
```
select
  max(_etl_loaded_at) as max_loaded_at,
  convert_timezone('UTC', current_timestamp()) as snapshotted_at
from raw.jaffle_shop.orders
```

### Incremental Model
1. What is an incremental model? 

    Incremental models are built as tables in your data warehouse – the first time a model is run, the table is built by transforming all rows of source data. On subsequent runs, dbt transforms only the rows in your source data that you tell dbt to filter for, inserting them into the table that has already been built (the target table).

    Often, the rows you filter for on an incremental run will be the rows in your source data that have been created or updated since the last time dbt ran. As such, on each dbt run, your model gets built incrementally.

2. Creating incremental models 
```
{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

SELECT  * EXCEPT(row_num)
FROM    ( SELECT  *
                , ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY last_updated_ts DESC) AS row_num
          FROM {{ ref('tdc__patron_order__base') }}
          {% if is_incremental() %}
              WHERE last_updated_ts > (SELECT MAX(last_updated_ts) FROM {{ this }})
          {% endif %}
)
WHERE   row_num = 1
--{{ this }} can be thought of as equivalent to ref('<the_current_model>'), and is a neat way to avoid circular dependencies
```
Behind the scene, a merge statement is used to insert new records and update existing records.
```
SELECT  * EXCEPT(row_num)
FROM    ( SELECT  *
                , ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY last_updated_ts DESC) AS row_num
          FROM __dbt__CTE__tdc__patron_order__base
          
              WHERE last_updated_ts > (SELECT MAX(last_updated_ts) FROM `brs-clouddata-prod`.`dbt_woody`.`tdc__patron_order__latest`)
          
)
WHERE   row_num = 1
```
The is_incremental() macro will return True if:
```
a. The destination table already exists in the database
b. dbt is not running in full-refresh mode
c. The running model is configured with materialized='incremental'
```
**NOTE: If your incremental model logic has changed, the transformations on your new rows of data may diverge from the historical transformations, which are stored in your target table. In this case, you should rebuild your incremental model. (using --full-refresh flag when running locally, remove the incremental table in BQ when running in production)**

## Common git command for a Simple workflow
1. Check the current branch
    ```
    git branch
    ```
  
2. Make sure you have pulled latest version of `master` as starting a new branch
    ```
    git checkout master ##switch to master branch
    git pull 
    ```
3. Create a new branch. (Make sure you have started a new branch to develop, not working on `master`)
    ```
    git checkout -b branch-name
    ```
4. Add files to staging and commit changes
    ```
    git status ##List the files you've changed and those you still need to add or commit
    git add <filename>  ##Add one file to staging
    git add .  ##Add all files to staging
    git commit -m 'message'
    ```
5. Send changes to the `master` branch of the remost repo
    ```
    git push
    git push --set-upstream origin branch-name ##if this is the first time you push a branch created locally
    ```
6. If there have been new changes added to `master` since you worked on the new branch, pull the new changes to `master`, and merge them to your branch, before merging your branch to `master`
    ```
    git checkout master
    git pull
    git checkout branch-name
    git merge master
    ```
    Note: you may receive a message like below. It’s not a git error message, but it’s the editor git uses your default editor.
    ```
    Merge branch 'master' of https://gitlab/..
    # Please enter a commit message to explain why this merge is necessary,
    # especially if it merges an updated upstream into a topic branch.
    #
    # Lines starting with '#' will be ignored, and an empty message aborts
    # the commit.
    ```
    There are several ways to resolve it, a simple is to use `:wq` (write & quit), and it writes a message "`Merge branch 'master' into branch-name`" after you do `git push` to finanlize it 
 7. These are the most common git command and workflow one may use in a nutshell. Feel free to reach out or learn more commands to enrich use cases!
     

## Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](http://slack.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices








