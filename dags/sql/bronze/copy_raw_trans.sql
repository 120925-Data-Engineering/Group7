COPY INTO BRONZE.RAW_TRANSACTIONS(raw, source_file, row_num)
    FROM(
        SELECT
            $1 as raw,
            METADATA$FILENAME as source_file,
            METADATA$FILE_ROW_NUMBER as row_num
        FROM @BRONZE.SPARK_STAGE/transactions/
    )
    FILE_FORMAT=(FORMAT_NAME = BRONZE.file_parquet)
    ON_ERROR='ABORT_STATEMENT';