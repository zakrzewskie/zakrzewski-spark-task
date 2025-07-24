# Raw Contracts data sample
CONTRACT_csv = """
SOURCE_SYSTEM,CONTRACT_ID,"CONTRACT_TYPE",INSURED_PERIOD_FROM,INSURED_PERIOD_TO,CREATION_DATE
Contract_SR_Europa_3,408124123,"Direct",01.01.2015,01.01.2099,17.01.2022 13:42
Contract_SR_Europa_3,46784575,"Direct",01.01.2015,01.01.2099,17.01.2022 13:42
Contract_SR_Europa_3,97563756,"",01.01.2015,01.01.2099,17.01.2022 13:42
Contract_SR_Europa_3,13767503,"Reinsurance",01.01.2015,01.01.2099,17.01.2022 13:42
Contract_SR_Europa_3,656948536,"",01.01.2015,01.01.2099,17.01.2022 13:42
"""

# Raw Claims data sample
CLAIM_csv = """
SOURCE_SYSTEM,CLAIM_ID,CONTRACT_SOURCE_SYSTEM,CONTRAT_ID,CLAIM_TYPE,DATE_OF_LOSS,AMOUNT,CREATION_DATE
Claim_SR_Europa_3,CL_68545123,Contract_SR_Europa_3,97563756,2,14.02.2021,523.21,17.01.2022 14:45
Claim_SR_Europa_3,CL_962234,Contract_SR_Europa_4,408124123,1,30.01.2021,52369.0,17.01.2022 14:46
Claim_SR_Europa_3,CL_895168,Contract_SR_Europa_3,13767503,,02.09.2020,98465,17.01.2022 14:45
Claim_SR_Europa_3,CX_12066501,Contract_SR_Europa_3,656948536,2,04.01.2022,9000,17.01.2022 14:45
Claim_SR_Europa_3,RX_9845163,Contract_SR_Europa_3,656948536,2,04.06.2015,11000,17.01.2022 14:45
Claim_SR_Europa_3,CL_39904634,Contract_SR_Europa_3,656948536,2,04.11.2020,11000,17.01.2022 14:46
Claim_SR_Europa_3,U_7065313,Contract_SR_Europa_3,46589516,1,29.09.2021,11000,17.01.2022 14:46
"""

def get_raw_data(csv, spark):
    # Convert string to RDD
    rdd = spark.sparkContext.parallelize(csv.strip().split("\n"))

    # Read as DataFrame
    df = spark.read.csv(rdd, header=True, inferSchema=False)

    return df
