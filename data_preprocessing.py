# Paqueterías
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Valida que los datos de un dataframe sean de cierto tipo especificado
def validate_dtypes(dtypes_series: pd.core.series.Series, dtype_dict: dict, date_columns: list = []) -> tuple[bool, str]:
  validation_message: str = "Successful validation"

  for column in dtypes_series.index:

    column_type = dtypes_series[column].type

    if (column in dtype_dict.keys()) and (column_type != dtype_dict[column]):
      validation_message = f"{column} type is {dtypes_series[column]}, but the expected type is {dtype_dict[column]}"
      return (False, validation_message)

    elif (column in date_columns) and (dtypes_series[column].type != np.datetime64):
      validation_message = f"{column} type is {dtypes_series[column]}, but the expected type is datetime64[ns]"
      return (False, validation_message)

  return (True, validation_message)

# Compara si los elementos de la columna de la tabla "izquierda" se encuentran
# en otra columna de la tabla "derecha"
def left_compare(left_df, left_val, right_df, right_val, indicator_name="_merge"):
  merged = pd.merge(pd.DataFrame(left_df[left_val].unique(),columns=[left_val]), right_df[right_val], left_on=left_val, right_on=right_val, how="left", indicator=indicator_name)
  merged[indicator_name] = merged[indicator_name].map({"left_only":False, "both":True})
  merged = merged.set_index(left_val)
  assert merged.shape[0] == left_df.shape[0], f"{merged.shape[0] - left_df.shape[0]}"
  return merged

# Procesamiento del reporte general de usuarios
def procesar_reporte_general_de_usuarios(path):
  objects = [
      "USER_ID",
      "ID_UNICO_ANDREA",
      "NOMBRE",
      "APELLIDO_PATERNO",
      "APELLIDO_MATERNO",
      "PERFIL",
      "CORREO_ELECTRONICO",
      "CELULAR",
      "ESTADO",
      "SEXO",
      "ESTATUS",
      "RFC"
  ]

  floats = [
      "NIVEL",
      "JOYAS_TOTALES_GANADAS",
      "JOYAS_CANJEADOS",
      "JOYAS_DISPONIBLES",
  ]

  dates = [
      "FECHA_DE_NACIMIENTO",
      "FECHA_COMPLEMENTO_DATOS",
      "ULTIMO_INGRESO_APP",
      "FECHA_CREACIÓN_DE_USUARIO"
  ]

  na_values_list = [
      "",
      "null",
      "NO UPDATE",
      "0000-00-00"
  ]

  dtype_dict = {column:np.object_ for column in objects} | {column:np.float64 for column in floats}

  RGU = pd.read_csv(
      path,
      encoding="latin-1",
      dtype=dtype_dict,
      na_values=na_values_list,
      parse_dates=dates
  )

  dtypes_validation = validate_dtypes(RGU.dtypes, dtype_dict, dates)
  assert dtypes_validation[0], dtypes_validation[1]

  RGU["Ganadoras"] = RGU["JOYAS_TOTALES_GANADAS"].apply(lambda x: "No ganadora" if x==0 else "Ganadora")
  RGU["Con canje"] = RGU["JOYAS_CANJEADOS"].apply(lambda x: "Sin canje" if x==0 else "Con canje")
  RGU["Con ingreso"] = RGU["ULTIMO_INGRESO_APP"].apply(lambda x: "Sin ingreso" if x==0 else "Con ingreso")

  generaciones_bins_dates = [
      "1/1/1900",
      "12/31/1964",
      "12/31/1976",
      "12/31/1995",
      "12/31/2050"
  ]

  generaciones_bins = pd.Series(generaciones_bins_dates,dtype="datetime64[ns]")

  generaciones_labels = [
      "Baby boomer",
      "Generacion X",
      "Millenial",
      "Z y otra"
  ]

  RGU["generación"] = pd.cut( RGU["FECHA_DE_NACIMIENTO"], bins=generaciones_bins, labels=generaciones_labels )

  return RGU

# Procesamiento del reporte de metas y resultados
def procesar_reporte_metas_y_resultados(path, reporte_general_de_usuarios):
  objects = [
      "ID_UNICO_ANDREA",
      "NOMBRE",
      "APELLIDO_PATERNO",
      "APELLIDO_MATERNO",
      "PERFIL",
      "PORCENTAJE_DE_CUMPLIMIENTO"
  ]

  floats = [
      "NIVEL",
      "HABILITADO",
      "ACTIVADO",
      "MES",
      "AÑO",
      "CUOTA_OBJETIVO",
      "MONTO_DE_VENTA_NETA_ACUMULADA_AL_CIERRE_DE_MES",
      "JOYAS_TOTALES_GANADAS_POR_COMPRAS"
  ]

  dtype_dict = {column:np.object_ for column in objects} | {column:np.float64 for column in floats}

  RMR = pd.read_csv(
      path,
      encoding="latin-1",
      dtype=dtype_dict
  )

  dtypes_validation = validate_dtypes(RMR.dtypes, dtype_dict)
  assert dtypes_validation[0], dtypes_validation[1]

  RMR["PORCENTAJE_DE_CUMPLIMIENTO"] = pd.to_numeric(RMR["PORCENTAJE_DE_CUMPLIMIENTO"].apply(lambda x: x.replace("%","")))

  RMR["Logro meta"] = RMR["PORCENTAJE_DE_CUMPLIMIENTO"].apply(lambda x: "Cumplió" if x >= 100 else "No cumplió")

  RMR["Crecimiento sobre la renta"] = RMR["MONTO_DE_VENTA_NETA_ACUMULADA_AL_CIERRE_DE_MES"] - RMR["CUOTA_OBJETIVO"]

  return pd.merge(RMR, left_compare(RMR, "ID_UNICO_ANDREA", reporte_general_de_usuarios, "ID_UNICO_ANDREA", indicator_name="Participa"), on="ID_UNICO_ANDREA")

# Procesamiento de datos de Shipping List
def procesar_shipping_list(path):
  objects = [
      "ID_UNICO_CANJE",
      "PEDIDO EREWARD",
      "EREWARD CANJE ID",
      "NOMBRE (S)",
      "APELLIDO PATERNO",
      "APELLIDO MATERNO",
      "ID_UNICO_ANDREA",
      "PERFIL",
      "CELULAR CANJE",
      "CORREO CANJE",
      "CORREO REGISTRADO",
      "CATEGORIA",
      "CLASIFICACION DE PRODUCTO",
      "NOMBRE CORTO",
      "SKU",
      "CODIGO PMR",
      "MARCA",
      "TELEFONO_DE_RECARGA",
      "COMPAÑÍA RECARGA",
      "PAQUETERIA",
      "LIGA_POD",
      "CALLE",
      "REFERENCIAS",
      "COLONIA",
      "MUNICIPIO",
      "ESTADO",
      "RFC MONEDERO",
      "NOMBRE COMPLETO MONEDERO",
      "LUGAR DE CANJE",
      "GUIA",
      "ESTATUS DE ENTREGA",
      "ID_ARTICULO_SAP",
      "QUIEN RECIBIO",
      "NÚMERO EXTERIOR",
      "NÚMERO INTERIOR"
  ]

  floats = [
      "PEDIDO",
      "PUNTOS",
      "CANTIDAD",
      "CODIGO POSTAL",
      "PRECIO LOGISTICA",
      "FEE",
      "PRECIO VENTA INTEGRADO",
      "PRECIO DE VENTA MAS AJUSTE",
      "MONTO DE DISPERSION",
      "COMISION PROVEEDOR",
      "PRECIO PLASTICO"
  ]

  dates = [
      "FECHA_DE_CANJE",
      "FECHA DE CORTE",
      "FECHA ESTIMADA DE ENTREGA",
      "FECHA DE RECEPCIÓN"
  ]

  na_values_list = ["", "NA", "s/n", np.nan, np.float64("nan")]

  dtype_dict = {column:np.object_ for column in objects} | {column:np.float64 for column in floats}

  try:
    SL = pd.read_csv(
        path,
        encoding="latin-1",
        dtype=dtype_dict,
        na_values=na_values_list,
    )
    SL[dates] = SL[dates].astype("datetime64[ns]")
  except KeyError:
    SL = pd.read_csv(
        path,
        encoding="utf-8",
        dtype=dtype_dict,
        na_values=na_values_list,
    )
    SL[dates] = SL[dates].astype("datetime64[ns]")
      
  SL["PRECIO PRODUCTO"] = SL["PRECIO PRODUCTO"].apply(lambda x: x.replace("$", "")).astype("float64")

  dtypes_validation = validate_dtypes(SL.dtypes, dtype_dict, dates)
  assert dtypes_validation[0], dtypes_validation[1]
  return SL

def distinct_count(dataframe, column, count_name="HEAD COUNT", return_=False):
  print(column,end="\n\n")
  distinct_count_df = pd.DataFrame([dataframe[column].nunique()], columns=[count_name])
  print( distinct_count_df )
  if return_:
    return distinct_count_df

def distinct_val_percentage(dataframe, column, decimals=0, print_count=True, return_=False, plot_percentages=False, plot_type=None):
  count_df = round(dataframe.rename(columns={column:f"{column}: porcentajes"})[f"{column}: porcentajes"].value_counts(1)*100, decimals)

  if print_count:
    print(count_df)
  else:
    return_ = True

  if plot_percentages:
    if plot_type == None:
      plot_type = "bar"

    if plot_type == "bar":
      ax0 = count_df.plot.bar(rot=False)
      for p0 in ax0.patches:
        ax0.annotate(str(round(p0.get_height(),2))+"%", (p0.get_x() + p0.get_width() / 2., p0.get_height()), ha='center', va='bottom')
      plt.show()

    elif plot_type == "pie":
      wedges, texts, autotexts = plt.pie( x=count_df, shadow=False, autopct='%1.0f%%', pctdistance=1.1 )
      plt.legend(wedges, count_df.index,
                title="Porcentajes",
                loc="center left",
                bbox_to_anchor=(1, 0, 0.5, 1))
      plt.show()
    else:
      print("Tipo de gráfico no reconocido o on disponible. Solo se acepta plot_type = 'bar' o 'pie'.")

  if return_:
    return count_df

def print_dataframe(dataframe):
  with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    print(dataframe)

def pivot_table(dataframe, rows, values=None, columns=None, margins=True, margins_name="Total", aggfunc="sum", rename_cols=None, return_=False):
  if values is None:
    if columns is None:
      return dataframe[rows].describe()
    else:
      if (dataframe[columns].dtypes.name == "object") or (dataframe[columns].dtypes.name == "datetime64[ns]"):
        dataframe_copy = dataframe[[rows,columns]].copy()
        dataframe_copy["aux_column"] = range( dataframe_copy.shape[0] )
        pivot_table = pd.pivot_table(
            dataframe_copy,
            index=rows,
            values="aux_column",
            columns=columns,
            aggfunc="count",
            margins=margins,
            margins_name=margins_name
        )
        
      else:
        raise TypeError("Las columnas deben de contener solo valores categóricos, no numéricos.")

  else:
    pivot_table = pd.pivot_table(
        dataframe,
        index=rows,
        values=values,
        columns=columns,
        aggfunc=aggfunc,
        margins=margins,
        margins_name=margins_name
    )

  if rename_cols:
    pivot_table = pivot_table.rename(columns=rename_cols)

  print_dataframe(pivot_table)

  if return_:
    return pivot_table