def rename_columns_contract(df):
    df = df.withColumnRenamed("INSUDRED_PERIOD_TO", "INSURED_PERIOD_TO")
    return df

def rename_columns_claim(df):
    df = df.withColumnRenamed("CONTRAT_ID", "CONTRACT_ID")
    return df