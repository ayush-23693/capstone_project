{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['SOURCE_FILE','ROW_NUMBER']
) }}


with 

source as (

    select *,
            METADATA$FILENAME AS SOURCE_FILE,
            METADATA$FILE_ROW_NUMBER AS ROW_NUMBER
    from {{ source('bronze', 'customer_data') }}

),

renamed as (

    select
        RAW_DATA,
        SOURCE_FILE,
        ROW_NUMBER,
        CURRENT_TIMESTAMP() AS LOADED_AT,
        '{{ invocation_id }}' AS BATCH_ID

    from source

)

select * from renamed