import polars as pl
from datetime import datetime
from datetime import date
import pyarrow as pa
import pyarrow.dataset
import random
import string

#read parquet files ft01
ft01_or425_2016_df = pl.read_parquet("./files/input/remaining/ft01/*")
#display(ft01_or425_2016_df.head())

#separate by year
ft01_or573_2021_df = ft01_or425_2016_df.filter((pl.col("ANIO_VIGENCIA") > 2021))
#ft01_or573_2021_df.select(pl.count())

#set uvt
ft01_or573_2021_df = ft01_or573_2021_df.with_columns(pl
                                                     .when(pl.col("ANIO_VIGENCIA") > 2022)
                                                     .then(42412)
                                                     .otherwise(38004)
                                                     .alias("UVT_SMMLV")) #uvt +2021



#TESCC post 2021
ft01_or573_2021_df = ft01_or573_2021_df.with_columns(pl
                                                     .when(pl.col("DESTINO_ECONOMICO_PREDIO") == "INDUSTRIAL")
                                                     .then(
                                                         pl
                                                         .when((pl.col("CONSUMO_ENERGIA_SIN_SUBSIDIO") * pl.lit(0.017)) > (pl.lit(42.54) * pl.col("UVT_SMMLV")))
                                                         .then((pl.lit(42.54) * pl.col("UVT_SMMLV")))
                                                         .otherwise((pl.col("CONSUMO_ENERGIA_SIN_SUBSIDIO") * pl.lit(0.017)))
                                                     )

                                                     .when(pl.col("DESTINO_ECONOMICO_PREDIO") == "COMERCIAL")
                                                     .then(
                                                         pl
                                                         .when((pl.col("CONSUMO_ENERGIA_SIN_SUBSIDIO") * pl.lit(0.017)) > (pl.lit(35) * pl.col("UVT_SMMLV")))
                                                         .then((pl.lit(35) * pl.col("UVT_SMMLV")))
                                                         .otherwise((pl.col("CONSUMO_ENERGIA_SIN_SUBSIDIO") * pl.lit(0.017)))
                                                     )
                                                     .when(pl.col("DESTINO_ECONOMICO_PREDIO") == "ESPECIAL")
                                                     .then(pl.lit(0.017) * pl.col("CONSUMO_ENERGIA_SIN_SUBSIDIO"))
                                                     .when(pl.col("DESTINO_ECONOMICO_PREDIO") == "OTROS")
                                                     .then(pl.lit(0.017) * pl.col("CONSUMO_ENERGIA_SIN_SUBSIDIO"))
                                                     .when(pl.col("DESTINO_ECONOMICO_PREDIO") == "OFICIAL - PUBLICO")
                                                     .then(pl.lit(0.01) * pl.col("CONSUMO_ENERGIA_SIN_SUBSIDIO"))
                                                     .when(pl.col("DESTINO_ECONOMICO_PREDIO") == "ESTRATO 4")
                                                     .then(pl.lit(0.01) * pl.col("CONSUMO_ENERGIA_SIN_SUBSIDIO"))
                                                     .when(pl.col("DESTINO_ECONOMICO_PREDIO") == "ESTRATO 5")
                                                     .then(pl.lit(0.02) * pl.col("CONSUMO_ENERGIA_SIN_SUBSIDIO"))
                                                     .when(pl.col("DESTINO_ECONOMICO_PREDIO") == "ESTRATO 6")
                                                     .then(pl.lit(0.02) * pl.col("CONSUMO_ENERGIA_SIN_SUBSIDIO"))
                                                     .otherwise(pl.lit(0.01) * pl.col("CONSUMO_ENERGIA_SIN_SUBSIDIO")).alias("TESCC"))


#low limit uvt
ft01_or573_2021_df = ft01_or573_2021_df.with_columns(pl
                                                     .when(pl.col("TESCC") < (pl.lit(0.05) * pl.col("UVT_SMMLV")))
                                                     .then((pl.lit(0.05) * pl.col("UVT_SMMLV")))
                                                     .otherwise(pl.col("TESCC"))
                                                     .alias("TESCC"))

#display(ft01_or573_2021_df.head())

#separate by year
ft01_or425_2016_df = ft01_or425_2016_df.filter((pl.col("ANIO_VIGENCIA") <= 2021))
#ft01_or425_2016_df.select(pl.count())

#set smmlv
ft01_or425_2016_df = ft01_or425_2016_df.with_columns(pl
                                                     .when(pl.col("ANIO_VIGENCIA") == 2017)
                                                     .then(737717)
                                                     .when(pl.col("ANIO_VIGENCIA") == 2018)
                                                     .then(781242)
                                                     .when(pl.col("ANIO_VIGENCIA") == 2019)
                                                     .then(828116)
                                                     .when(pl.col("ANIO_VIGENCIA") == 2020)
                                                     .then(877803)
                                                     .when(pl.col("ANIO_VIGENCIA") == 2021)
                                                     .then(908526)
                                                     .otherwise(908526).alias("UVT_SMMLV")) #smmlv -2021


#TESCC before 2021
ft01_or425_2016_df = ft01_or425_2016_df.with_columns(pl
                                                     .when(pl.col("DESTINO_ECONOMICO_PREDIO") == "INDUSTRIAL")
                                                     .then(
                                                         pl
                                                         .when(pl.col("CONSUMO_KWH") >= 300000)
                                                         .then(pl.lit(1.7) * pl.col("UVT_SMMLV"))
                                                         .otherwise(pl.lit(0.017) * pl.col("CONSUMO_ENERGIA_SIN_SUBSIDIO"))
                                                     )
                                                     .when(pl.col("DESTINO_ECONOMICO_PREDIO") == "COMERCIAL")
                                                     .then(
                                                         pl
                                                         .when(pl.col("CONSUMO_KWH") >= 200000)
                                                         .then(pl.lit(1.4) * pl.col("UVT_SMMLV"))
                                                         .otherwise(pl.lit(0.017) * pl.col("CONSUMO_ENERGIA_SIN_SUBSIDIO"))
                                                     )
                                                     .when(pl.col("DESTINO_ECONOMICO_PREDIO") == "ESPECIAL")
                                                     .then(pl.lit(0.017) * pl.col("CONSUMO_ENERGIA_SIN_SUBSIDIO"))
                                                     .when(pl.col("DESTINO_ECONOMICO_PREDIO") == "OTROS")
                                                     .then(pl.lit(0.017) * pl.col("CONSUMO_ENERGIA_SIN_SUBSIDIO"))
                                                     .when(pl.col("DESTINO_ECONOMICO_PREDIO") == "OFICIAL - PUBLICO")
                                                     .then(pl.lit(0.01) * pl.col("CONSUMO_ENERGIA_SIN_SUBSIDIO"))
                                                     .when(pl.col("DESTINO_ECONOMICO_PREDIO") == "ESTRATO 4")
                                                     .then(pl.lit(0.01) * pl.col("CONSUMO_ENERGIA_SIN_SUBSIDIO"))
                                                     .when(pl.col("DESTINO_ECONOMICO_PREDIO") == "ESTRATO 5")
                                                     .then(pl.lit(0.02) * pl.col("CONSUMO_ENERGIA_SIN_SUBSIDIO"))
                                                     .when(pl.col("DESTINO_ECONOMICO_PREDIO") == "ESTRATO 6")
                                                     .then(pl.lit(0.02) * pl.col("CONSUMO_ENERGIA_SIN_SUBSIDIO"))
                                                     .otherwise(pl.lit(0.01) * pl.col("CONSUMO_ENERGIA_SIN_SUBSIDIO")).alias("TESCC"))

#display(ft01_or425_2016_df.head())



#union ft01
ft01_df = pl.concat([ft01_or425_2016_df, ft01_or573_2021_df])
#display(ft01_df.head())


#read parquet files ft03
ft03_df = pl.read_parquet("./files/input/remaining/ft03/*")
ft03_df = ft03_df.with_columns(pl.col("NUMERO_FACTURA").alias("NUMERO_FACTURA_FT03"))
#display(ft03_df.head())


# report
df_report = ft01_df.join(ft03_df, on="NUMERO_FACTURA")
#display(df_report.head())

#saldo cartera
df_report = df_report.with_columns((pl.col("TESCC") - pl.col("VALOR_RECAUDO")).alias('CARTERA FT01-FT03'))

# Getting rows in FT01 only if not present in FT03
df_exclude = ft01_df.join(ft03_df, on="NUMERO_FACTURA", how="left").filter(pl.all(pl.col('NUMERO_FACTURA_FT03').is_null()))
#display(df_exclude)


# filenames
letters_var = string.ascii_letters
success_var =  ( ''.join(random.choice(letters_var) for i in range(15)) )
error_var = ( ''.join(random.choice(letters_var) for i in range(15)) )

#save report
#to csv
df_report.write_csv("./files/output/completed/found_num_fact/report_" + success_var + ".csv")

#save not found records
#to csv
df_exclude.write_csv("./files/output/completed/not_found/not_found_" + error_var + ".csv")

